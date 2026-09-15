use crate::{host::Host, tls::TlsFactory, worker::tcp::TcpWorker, VetisHosts};
use log::{debug, error, info};
use papaya::HashMap;
use std::{net::SocketAddr, sync::Arc};
use tokio_rustls::TlsAcceptor;
use tokio_util::sync::CancellationToken;
use vetis::{
    errors::{ListenerError, VetisError},
    host::Host as _,
    listener::ListenerConfig,
    Alpn, VetisResult,
};

/// TCP listener
pub struct TcpListener {
    config: ListenerConfig,
    pub(crate) hosts: VetisHosts<Host>,
    token: Option<CancellationToken>,
    inner: Option<tokio::net::TcpListener>,
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
        Self { config, hosts: VetisHosts::new(HashMap::new()), token: None, inner: None }
    }
}

impl vetis::listener::Listener for TcpListener {
    type RuntimeHost = Host;

    /// Add a new host
    ///
    /// # Arguments
    ///
    /// * `host` - A host instance.
    fn add_host(&mut self, host: Arc<Self::RuntimeHost>) -> VetisResult<()> {
        // Add a host
        let hosts = self
            .hosts
            .pin_owned();
        hosts.insert(format!("{}:{}", host.hostname(), self.config().port()), host.clone());
        Ok(())
    }

    /// Add a new host
    ///
    /// # Arguments
    ///
    /// * `host` - A host instance.
    fn remove_host(&mut self, hostname: &str) -> VetisResult<()> {
        let hosts = self
            .hosts
            .pin_owned();
        hosts.remove(hostname);
        Ok(())
    }

    /// Return totan number of hosts assigned to this listener
    fn total_hosts(&self) -> usize {
        self.hosts.len()
    }

    /// Reserve port by OS
    async fn reserve_port(&mut self) -> VetisResult<()> {
        if self.config.port() == 0 {
            let listener = tokio::net::TcpListener::bind((
                self.config
                    .interface()
                    .to_string(),
                self.config.port(),
            ))
            .await
            .map_err(|e| VetisError::Listener(ListenerError::Bind(e.to_string())))?;

            let local_addr = listener
                .local_addr()
                .map_err(|e| VetisError::Listener(ListenerError::Bind(e.to_string())))?;

            self.config
                .reassign_port(local_addr.port());

            self.inner = Some(listener);
        }

        Ok(())
    }

    fn config(&self) -> &ListenerConfig {
        &self.config
    }

    /// Listen for incoming connections
    ///
    /// # Returns
    ///
    /// * `ListenerResult<'_, ()>` - A `ListenerResult` instance containing the result of the listener.
    async fn listen(&mut self) -> VetisResult<()> {
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
                .map_err(|e| VetisError::Bind(e.to_string()))?
        };

        let dispatcher_token = CancellationToken::new();
        let token = dispatcher_token.clone();
        let mut dispatcher = ConnectionDispatcher::new(
            listener,
            self.hosts.clone(),
            self.config.clone(),
            dispatcher_token.child_token(),
        );
        tokio::spawn(async move {
            tokio::select! {
                _ = token.cancelled() => {
                    info!("Stopping listener...")
                }
                _ = dispatcher.dispatch_connections() => ()
            }
        });
        self.token = Some(dispatcher_token);

        Ok(())
    }

    /// Stop the listener
    ///
    /// # Returns
    ///
    /// * `ListenerResult<'_, ()>` - A `ListenerResult` instance containing the result of the listener.
    async fn stop(&mut self) -> VetisResult<()> {
        if let Some(token) = self.token.take() {
            token.cancel();
        }
        Ok(())
    }
}

struct ConnectionDispatcher {
    listener: tokio::net::TcpListener,
    hosts: VetisHosts<Host>,
    config: ListenerConfig,
    token: CancellationToken,
}

/// Decompose the TCP listener into smaller, more manageable structs
impl ConnectionDispatcher {
    pub fn new(
        listener: tokio::net::TcpListener,
        hosts: VetisHosts<Host>,
        config: ListenerConfig,
        token: CancellationToken,
    ) -> Self {
        Self { listener, hosts, config, token }
    }

    async fn dispatch_connections(&mut self) -> VetisResult<()> {
        // Limit supported alpns for TCP only
        let alpn_protocols: Vec<Vec<u8>> = self
            .config
            .alpn_protos()
            .iter()
            .filter(|val| match val {
                Alpn::AcmeTls1 | Alpn::Doh | Alpn::Dot | Alpn::Http11 | Alpn::H2 | Alpn::H2c => {
                    true
                }
                _ => false,
            })
            .map(|val| val.into())
            .collect();

        let tls_config = TlsFactory::create_tls_config(self.hosts.clone(), alpn_protocols).await?;
        let allow_plain_connection = self
            .config
            .allow_unsafe_connections();

        loop {
            let Ok((tcp_stream, _)) = self
                .listener
                .accept()
                .await
            else {
                error!(
                    "Cannot accept connection: {:?}",
                    self.listener
                        .accept()
                        .await
                        .err()
                );
                continue;
            };

            // TODO: Check ACL before proceeding
            let mut worker = TcpWorker::new(
                TlsAcceptor::from(tls_config.clone()),
                self.hosts.clone(),
                allow_plain_connection,
                self.token
                    .child_token(),
            );

            let token = self.token.clone();
            let handle = tokio::spawn(async move {
                tokio::select! {
                    _ = token.cancelled() => {
                        debug!("Worker stopping..");
                    },
                    _ = worker.run(tcp_stream) => {}
                }
            });

            match handle.await {
                Ok(_) => {
                    debug!("Worker successfully stopped..");
                    continue;
                }
                Err(e) => {
                    error!("Internal error: {:?}", e);
                    continue;
                }
            }
        }
    }
}
