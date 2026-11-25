use crate::{host::Host, service::http::HttpService, tls::TlsConfigResolver};
use compact_str::ToCompactString;
use hyper_util::{
    rt::{TokioExecutor, TokioIo},
    server::conn::auto,
};
use papaya::HashMap;
use peekable::tokio::AsyncPeekable;
use rustls::server::Acceptor;
use std::{hash::Hash, net::SocketAddr, sync::Arc};
use tokio::{
    net::TcpStream,
    sync::watch,
    task::{self, JoinHandle},
};
use tokio_rustls::LazyConfigAcceptor;
use vetis::{
    LogSender, VetisHosts, VetisResult, debug, error,
    errors::{ListenerError, VetisError},
    info,
    listener::ListenerConfig,
    log::Logger,
};

/// TCP listener
pub struct TcpListener {
    config: ListenerConfig,
    signal: Option<watch::Sender<bool>>,
    inner: Option<tokio::net::TcpListener>,
    logger: Option<Logger<LogSender>>,
    handle: Option<JoinHandle<VetisResult<()>>>,
    connections: Arc<HashMap<SocketAddr, JoinHandle<VetisResult<()>>>>,
}

impl Hash for TcpListener {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.config
            .port()
            .hash(state);
        self.config
            .interface()
            .hash(state);
    }
}

impl PartialEq for TcpListener {
    fn eq(&self, other: &Self) -> bool {
        self.config.port() == other.config.port()
            && self
                .config
                .interface()
                == other
                    .config
                    .interface()
    }

    fn ne(&self, other: &Self) -> bool {
        self.config.port() != other.config.port()
            || self
                .config
                .interface()
                != other
                    .config
                    .interface()
    }
}

impl TcpListener {
    /// Create a new listener
    ///
    /// # Arguments
    ///
    /// * `config` - A `ListenerConfig` instance containing the listener configuration.
    ///
    /// # Returns
    ///
    /// * `Self` - A new `TcpListener` instance.
    pub fn new(config: ListenerConfig) -> Self {
        Self {
            config,
            signal: None,
            inner: None,
            logger: None,
            handle: None,
            connections: HashMap::new().into(),
        }
    }

    async fn accept(
        &mut self,
        tcp_stream: TcpStream,
        client_addr: SocketAddr,
        hosts: VetisHosts<Host>,
        tls_config_resolver: Arc<TlsConfigResolver>,
    ) -> VetisResult<()> {
        let (shut_send, _) = watch::channel(false);
        self.signal = Some(shut_send.clone());

        let mut peekable = AsyncPeekable::from(tcp_stream);
        let mut buf = [0; 2];
        peekable
            .peek_exact(&mut buf)
            .await
            .map_err(|e| VetisError::tcp(e.to_compact_string()))?;

        let service = HttpService::new(hosts.clone(), client_addr, self.logger.clone());
        let is_tls = buf.starts_with(&[0x16, 0x03]);
        let handle = if is_tls {
            let logger = self.logger.clone();
            let conns = self
                .connections
                .clone();
            let mut shut_signal = shut_send.subscribe();
            tokio::spawn(async move {
                let start_handshake = LazyConfigAcceptor::new(Acceptor::default(), peekable)
                    .await
                    .map_err(|e| VetisError::tls(e.to_compact_string()))?;
                let client_hello = start_handshake.client_hello();
                let sni = client_hello
                    .server_name()
                    .unwrap_or("://example.com");

                let server_config: Arc<rustls::ServerConfig> = if let Some(config) =
                    tls_config_resolver
                        .hosts_tls_config
                        .get(sni)
                {
                    config.clone()
                } else {
                    return Err(VetisError::tls("Missing host TLS configuration"));
                };

                let tls_stream = start_handshake
                    .into_stream(server_config)
                    .await
                    .map_err(|e| VetisError::tls(e.to_compact_string()))?;

                let builder = auto::Builder::new(TokioExecutor::new());
                tokio::select! {
                    _ = shut_signal.changed() => {
                        info!(logger, "Closing connection {}...", &client_addr);
                        Ok(())
                    }
                    res = builder.serve_connection_with_upgrades(TokioIo::new(tls_stream), service) => {
                        conns.pin().remove(&client_addr);
                        res.map_err(|e| VetisError::tls(e.to_compact_string()))
                    }
                }
            })
        } else {
            let logger = self.logger.clone();
            let conns = self
                .connections
                .clone();
            let mut shut_signal = shut_send.subscribe();
            task::spawn(async move {
                let builder = auto::Builder::new(TokioExecutor::new());
                tokio::select! {
                    _ = shut_signal.changed() => {
                        info!(logger, "Closing connection {}...", client_addr);
                        Ok(())
                    }
                    res = builder.serve_connection_with_upgrades(TokioIo::new(peekable), service) => {
                        conns.pin().remove(&client_addr);
                        res.map_err(|e| VetisError::tcp(e.to_compact_string()))
                    }
                }
            })
        };

        self.connections
            .pin()
            .insert(client_addr, handle);

        Ok(())
    }
}

impl vetis::listener::Listener for TcpListener {
    type RuntimeHost = Host;
    type Logger = Logger<LogSender>;

    fn logger(&mut self, logger: Self::Logger) {
        self.logger = Some(logger);
    }

    async fn reserve_port(&mut self) -> VetisResult<()> {
        if self.config.port() == 0 {
            let listener = tokio::net::TcpListener::bind((
                self.config
                    .interface()
                    .to_string(),
                self.config.port(),
            ))
            .await
            .map_err(|e| VetisError::Listener(ListenerError::Bind(e.to_compact_string())))?;

            let local_addr = listener
                .local_addr()
                .map_err(|e| VetisError::Listener(ListenerError::Bind(e.to_compact_string())))?;

            self.config
                .reassign_port(local_addr.port());

            self.inner = Some(listener);
        }

        Ok(())
    }

    fn reassign_port(&mut self, port: u16) {
        self.config
            .reassign_port(port);
    }

    fn config(&self) -> &ListenerConfig {
        &self.config
    }

    async fn listen(&mut self, hosts: VetisHosts<Self::RuntimeHost>) -> VetisResult<()> {
        info!(&self.logger, "TCP listener started...");
        let addr = SocketAddr::new(
            *self
                .config
                .interface(),
            self.config.port(),
        );

        let listener = if let Some(listener) = self.inner.take() {
            listener
        } else {
            tokio::net::TcpListener::bind(addr)
                .await
                .map_err(|e| VetisError::bind(e.to_compact_string()))?
        };

        let tls_config_resolver = Arc::new(TlsConfigResolver::create_tls_config(hosts.clone())?);
        loop {
            let result = listener
                .accept()
                .await;

            let Ok((tcp_stream, _)) = result else {
                debug!(self.logger, "Cannot accept connection: {:?}", result.err());
                continue;
            };

            let Ok(client_addr) = tcp_stream.peer_addr() else {
                return Err(VetisError::tcp("Unkown client address"));
            };

            self.accept(tcp_stream, client_addr, hosts.clone(), tls_config_resolver.clone())
                .await?;
        }
    }

    async fn stop(mut self) -> VetisResult<()> {
        if let Some(signal) = self.signal.take() {
            let _ = signal.send(true);
            if let Some(handle) = self.handle.take() {
                match handle.await {
                    Ok(_) => info!(&self.logger, "TCP Listener stopped successfully!"),
                    Err(e) => {
                        error!(self.logger, "Error while stopping listener: {}", e.to_string())
                    }
                }
            }
        }

        for handle in self
            .connections
            .pin()
            .values()
        {
            handle.abort();
        }

        Ok(())
    }
}
