use crate::{host::Host, service::quic::HttpService, tls::TlsConfigResolver};
use bytes::Bytes;
use compact_str::ToCompactString;
use futures_util::StreamExt as _;
use h3::{
    ext::Protocol,
    server::{self, RequestStream},
};
use h3_quinn::{
    Connection as QuinnConnection,
    quinn::{self, crypto::rustls::QuicServerConfig},
};
use http::{Method, Request};
use hyper::service::Service as _;
use hyper_body_utils::HttpBody;
use papaya::HashMap;
use quinn::{Endpoint, EndpointConfig, default_runtime};
use std::{hash::Hash, net::SocketAddr, sync::Arc};
use tokio::{
    net::UdpSocket,
    sync::watch,
    task::{self, JoinHandle},
};
use vetis::{
    LogSender, VetisHosts, VetisResult, debug, error,
    errors::{ListenerError, StartupError, VetisError},
    info,
    listener::ListenerConfig,
    log::Logger,
};

/// UDP listener
pub struct UdpListener {
    config: ListenerConfig,
    signal: Option<watch::Sender<bool>>,
    inner: Option<UdpSocket>,
    logger: Option<Logger<LogSender>>,
    handle: Option<JoinHandle<VetisResult<()>>>,
    connections: Arc<HashMap<SocketAddr, JoinHandle<VetisResult<()>>>>,
}

impl Hash for UdpListener {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.config
            .port()
            .hash(state);
        self.config
            .interface()
            .hash(state);
    }
}

impl PartialEq for UdpListener {
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
            && self
                .config
                .interface()
                != other
                    .config
                    .interface()
    }
}

impl UdpListener {
    /// Create a new listener
    ///
    /// # Arguments
    ///
    /// * `config` - A `ListenerConfig` instance containing the listener configuration.
    ///
    /// # Returns
    ///
    /// * `Self` - A new `UdpListener` instance.
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
}

impl vetis::listener::Listener for UdpListener {
    type RuntimeHost = Host;
    type Logger = Logger<LogSender>;

    fn logger(&mut self, logger: Self::Logger) {
        self.logger = Some(logger);
    }

    async fn reserve_port(&mut self) -> VetisResult<()> {
        if self.config.port() == 0 {
            let listener = UdpSocket::bind((
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
        info!(&self.logger, "UDP listener started...");
        let socket = if let Some(socket) = self.inner.take() {
            socket
        } else {
            let addr = SocketAddr::new(
                *self
                    .config
                    .interface(),
                self.config.port(),
            );
            tokio::net::UdpSocket::bind(addr)
                .await
                .map_err(|e| VetisError::bind(e.to_compact_string()))?
        }
        .into_std()
        .map_err(|e| VetisError::bind(e.to_compact_string()))?;

        let logger = self.logger.clone();

        let tls_config = TlsConfigResolver::create_tls_config(hosts.clone())?;

        let quic_config = QuicServerConfig::try_from()
            .map_err(|e| VetisError::Startup(StartupError::Tls(e.to_compact_string())))?;
        let server_config = quinn::ServerConfig::with_crypto(Arc::new(quic_config));

        let runtime =
            default_runtime().ok_or_else(|| VetisError::bind("no async runtime found"))?;

        let listener =
            Endpoint::new(EndpointConfig::default(), Some(server_config), socket, runtime)
                .map_err(|e| VetisError::bind(e.to_compact_string()))?;

        while let Some(new_conn) = listener
            .accept()
            .await
        {
            let udp_stream = new_conn
                .await
                .map_err(|e| VetisError::worker(e.to_compact_string()))?;

            let client_addr = udp_stream.remote_address();
            let service = HttpService::new(hosts.clone(), client_addr, self.logger.clone());
            let logger = logger.clone();
            let conns = self
                .connections
                .clone();

            let (shut_send, _) = watch::channel(false);
            let mut shut_signal = shut_send.subscribe();
            let handle = task::spawn(async move {
                let builder = Builder::new()
                    .enable_datagram(true)
                    .enable_extended_connect(true)
                    .enable_webtransport(true)
                    .max_webtransport_sessions(1);
                tokio::select! {
                    _ = shut_signal.changed() => {
                        info!(&logger, "Closing connection {}...", &client_addr);
                        Ok(())
                    }
                    res = builder.serve_connection(udp_stream, service, logger.clone()) => {
                        conns.pin().remove(&client_addr);
                        res.map_err(|e| VetisError::worker(e.to_compact_string()))
                    }
                }
            });

            self.connections
                .pin()
                .insert(client_addr, handle);
        }

        listener
            .wait_idle()
            .await;

        Ok(())
    }

    async fn stop(mut self) -> VetisResult<()> {
        if let Some(signal) = self.signal.take() {
            let _ = signal.send(true);
            if let Some(handle) = self.handle.take() {
                match handle.await {
                    Ok(_) => info!(&self.logger, "UDP Listener stopped successfully!"),
                    Err(e) => {
                        error!(self.logger, "Error while stopping listener: {}", e.to_string())
                    }
                }
            }
        }
        Ok(())
    }
}

pub(crate) struct Builder {
    enable_datagram: bool,
    enable_extended_connect: bool,
    enable_webtransport: bool,
    max_webtransport_sessions: u16,
}

impl Builder {
    pub(crate) fn new() -> Self {
        Self {
            enable_datagram: false,
            enable_extended_connect: false,
            enable_webtransport: false,
            max_webtransport_sessions: 0,
        }
    }

    pub(crate) fn enable_datagram(mut self, enabled: bool) -> Self {
        self.enable_datagram = enabled;
        self
    }

    pub(crate) fn enable_extended_connect(mut self, enabled: bool) -> Self {
        self.enable_extended_connect = enabled;
        self
    }

    pub(crate) fn enable_webtransport(mut self, enabled: bool) -> Self {
        self.enable_webtransport = enabled;
        self
    }

    pub(crate) fn max_webtransport_sessions(mut self, max: u16) -> Self {
        self.max_webtransport_sessions = max;
        self
    }

    async fn serve_raw(
        &self,
        request: Request<()>,
        stream: RequestStream<h3_quinn::BidiStream<Bytes>, Bytes>,
        service: &HttpService<Host>,
        logger: Option<Logger<LogSender>>,
    ) -> VetisResult<()> {
        let (parts, _) = request.into_parts();
        let body = HttpBody::server_bidi_stream(stream);
        let request = http::Request::from_parts(parts, body);

        let response = service
            .call(request)
            .await;

        if let Err(err) = response {
            error!(logger, "Error serving connection: {:?}", err);
            return Err(VetisError::worker(err.to_compact_string()));
        }

        Ok(())
    }

    async fn serve_http(
        &self,
        request: Request<()>,
        stream: RequestStream<h3_quinn::BidiStream<Bytes>, Bytes>,
        service: &HttpService<Host>,
        logger: Option<Logger<LogSender>>,
    ) -> VetisResult<()> {
        let (mut send_stream, recv_stream) = stream.split();
        let (parts, _) = request.into_parts();
        let body = HttpBody::server_recv_stream(recv_stream);
        let request = http::Request::from_parts(parts, body);

        let response = service
            .call(request)
            .await;

        if let Err(err) = response {
            error!(logger, "Error serving connection: {:?}", err);
            return Err(VetisError::worker(err.to_compact_string()));
        }

        if let Ok(response) = response {
            let (parts, mut body) = response.into_parts();

            let mut resp = http::Response::builder()
                .status(parts.status)
                .version(parts.version)
                .extension(parts.extensions)
                .body(())
                .unwrap();

            resp.headers_mut()
                .extend(parts.headers);

            match send_stream
                .send_response(resp)
                .await
            {
                Ok(_) => {
                    debug!(logger, "Response sent successfully!");
                }
                Err(err) => {
                    error!(logger, "Unable to send response: {:?}", err);
                }
            }

            while let Some(buf) = body.next().await {
                if let Ok(buf) = buf {
                    if let Ok(bytes) = buf.into_data() {
                        send_stream
                            .send_data(bytes)
                            .await
                            .map_err(|e| VetisError::worker(e.to_compact_string()))?;
                    }
                }
            }

            send_stream
                .finish()
                .await
                .map_err(|e| VetisError::worker(e.to_compact_string()))?;
        }

        Ok(())
    }

    async fn serve_connection(
        &self,
        conn: quinn::Connection,
        service: HttpService<Host>,
        logger: Option<Logger<LogSender>>,
    ) -> VetisResult<()> {
        let mut server_builder = server::builder();

        let mut h3_conn = server_builder
            .enable_datagram(true)
            .enable_extended_connect(true)
            .enable_webtransport(true)
            .max_webtransport_sessions(1)
            .build(QuinnConnection::new(conn))
            .await
            .map_err(|e| VetisError::worker(e.to_compact_string()))?;

        while let Some(resolver) = h3_conn
            .accept()
            .await
            .map_err(|e| VetisError::worker(e.to_compact_string()))?
        {
            if let Ok((req, stream)) = resolver
                .resolve_request()
                .await
            {
                let result = if req.method() == Method::CONNECT
                    && req
                        .extensions()
                        .get::<Protocol>()
                        == Some(&Protocol::WEB_TRANSPORT)
                {
                    self.serve_raw(req, stream, &service, logger.clone())
                        .await
                } else {
                    self.serve_http(req, stream, &service, logger.clone())
                        .await
                };

                if result.is_err() {
                    continue;
                }
            }
        }
        Ok(())
    }
}
