use crate::{VetisHosts, host::Host, tls::TlsFactory, worker::udp::UdpWorker};
use crossfire::{
    MAsyncRx, MAsyncTx,
    mpmc::{self, Array},
};
use h3_quinn::quinn::{self, crypto::rustls::QuicServerConfig};
use papaya::HashMap;
use quinn::{Connection, Endpoint};
use std::{hash::Hash, net::SocketAddr, sync::Arc};
use tokio::{
    sync::watch,
    task::{JoinHandle, JoinSet},
};
use vetis::{
    LogSender, VetisResult, error,
    errors::{ListenerError, StartError, VetisError},
    host::Host as _,
    info,
    listener::ListenerConfig,
    log::Logger,
};
/// UDP listener
pub struct UdpListener {
    config: ListenerConfig,
    hosts: VetisHosts<Host>,
    signal: Option<watch::Sender<bool>>,
    inner: Option<Endpoint>,
    logger: Option<Logger<LogSender>>,
    handle: Option<JoinHandle<()>>,
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
            hosts: VetisHosts::new(HashMap::new()),
            signal: None,
            inner: None,
            logger: None,
            handle: None,
        }
    }

    async fn create_inner_listener(&mut self) -> VetisResult<Endpoint> {
        let addr = SocketAddr::new(
            *self
                .config
                .interface(),
            self.config.port(),
        );

        let tls_config = TlsFactory::create_tls_config(self.hosts.clone()).await?;
        let quic_config = QuicServerConfig::try_from(tls_config)
            .map_err(|e| VetisError::Start(StartError::Tls(e.to_string())))?;
        let server_config = quinn::ServerConfig::with_crypto(Arc::new(quic_config));

        quinn::Endpoint::server(server_config, addr)
            .map_err(|e| VetisError::Listener(ListenerError::Bind(e.to_string())))
    }
}

impl vetis::listener::Listener for UdpListener {
    type RuntimeHost = Host;
    type Logger = Logger<LogSender>;

    fn add_host(&mut self, host: Arc<Self::RuntimeHost>) -> VetisResult<()> {
        // Add a host
        let hosts = self
            .hosts
            .pin_owned();
        hosts.insert(format!("{}:{}", host.hostname(), self.config().port()), host.clone());
        Ok(())
    }

    fn remove_host(&mut self, hostname: &str) -> VetisResult<()> {
        let hosts = self
            .hosts
            .pin_owned();
        hosts.remove(hostname);
        Ok(())
    }

    fn logger(&mut self, logger: Self::Logger) {
        self.logger = Some(logger);
    }

    fn total_hosts(&self) -> usize {
        self.hosts.len()
    }

    async fn reserve_port(&mut self) -> VetisResult<()> {
        if self.config.port() == 0 {
            let listener = self
                .create_inner_listener()
                .await?;

            let local_addr = listener
                .local_addr()
                .map_err(|e| VetisError::Listener(ListenerError::Bind(e.to_string())))?;

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

    async fn listen(&mut self) -> VetisResult<()> {
        info!(&self.logger, "UDP listener started...");
        let listener = if let Some(listener) = self.inner.take() {
            listener
        } else {
            self.create_inner_listener()
                .await?
        };

        let (shut_sender, shut_recv) = watch::channel(false);
        let mut shut_signal = shut_recv.clone();
        let logger = self.logger.clone();
        let mut dispatcher = ConnectionDispatcher::new(
            listener,
            self.hosts.clone(),
            self.config.clone(),
            self.logger.clone(),
        );
        let handle = tokio::spawn(async move {
            tokio::select! {
                _ = shut_signal.changed() => {
                    info!(logger, "Stopping listener...");
                    let _ = dispatcher.stop().await;
                }
                _ = dispatcher.run() => ()
            }
        });

        self.signal = Some(shut_sender);
        self.handle = Some(handle);

        Ok(())
    }

    async fn stop(&mut self) -> VetisResult<()> {
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

struct ConnectionDispatcher {
    listener: quinn::Endpoint,
    hosts: VetisHosts<Host>,
    signal: Option<watch::Sender<bool>>,
    config: ListenerConfig,
    logger: Option<Logger<LogSender>>,
    workers: JoinSet<()>,
    sender: Option<MAsyncTx<Array<Connection>>>,
}

impl ConnectionDispatcher {
    pub fn new(
        listener: quinn::Endpoint,
        hosts: VetisHosts<Host>,
        config: ListenerConfig,
        logger: Option<Logger<LogSender>>,
    ) -> Self {
        Self {
            listener,
            hosts,
            signal: None,
            config,
            logger,
            workers: JoinSet::new(),
            sender: None,
        }
    }

    async fn init(&mut self, receiver: MAsyncRx<Array<Connection>>) -> VetisResult<()> {
        info!(&self.logger, "Initializing udp workers...");
        let (shut_sender, shut_recv) = watch::channel(false);

        for worker_num in 1..=self
            .config
            .workers()
        {
            info!(&self.logger, "Initializing udp worker: {}", worker_num);
            let mut worker = UdpWorker::new(
                worker_num,
                self.hosts.clone(),
                self.logger.clone(),
                receiver.clone(),
            );

            let mut shut_signal = shut_recv.clone();
            let logger = self.logger.clone();
            let worker_future = async move {
                tokio::select! {
                    _ = shut_signal.changed() => {
                        info!(logger, "Stopping udp worker {}...", worker.id());
                        let _ = worker.stop().await;
                    },
                    _ = worker.run() => {}
                }
            };

            self.workers
                .spawn(worker_future);
        }

        self.signal = Some(shut_sender);

        Ok(())
    }

    async fn run(&mut self) -> VetisResult<()> {
        let (dispatch_sender, dispatch_recv) = mpmc::bounded_async::<Connection>(
            self.config
                .workers(),
        );

        if let Err(e) = self
            .init(dispatch_recv.clone())
            .await
        {
            error!(self.logger, "Could not start workers: {}", e.to_string())
        }

        self.sender = Some(dispatch_sender);

        while let Some(new_conn) = self
            .listener
            .accept()
            .await
        {
            let conn = new_conn
                .await
                .map_err(|e| VetisError::Worker(e.to_string()))?;

            if let Some(sender) = self.sender.as_ref()
                && let Err(e) = sender
                    .send(conn)
                    .await
            {
                error!(self.logger, "Could not distribute connection: {}", e.to_string())
            }
        }

        self.listener
            .wait_idle()
            .await;

        Ok(())
    }

    pub async fn stop(&mut self) -> VetisResult<()> {
        if let Some(signal) = self.signal.take() {
            let _ = signal.send(true);
        }
        while let Some(handle) = self
            .workers
            .join_next()
            .await
        {
            match handle {
                Ok(_) => {
                    info!(&self.logger, "UDP worker stopped successfully!");
                }
                Err(e) => {
                    error!(self.logger, "Internal error: {:?}", e);
                }
            }
        }

        Ok(())
    }
}
