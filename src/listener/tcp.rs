use crate::{VetisHosts, host::Host, tls::TlsFactory, worker::tcp::TcpWorker};
use crossfire::{
    MAsyncRx, MAsyncTx,
    mpmc::{self, Array},
};
use papaya::HashMap;
use std::{hash::Hash, net::SocketAddr, sync::Arc};
use tokio::{
    net::TcpStream,
    sync::watch,
    task::{JoinHandle, JoinSet},
};
use tokio_rustls::TlsAcceptor;
use vetis::{
    LogSender, VetisResult, debug, error,
    errors::{ListenerError, VetisError},
    host::Host as _,
    info,
    listener::ListenerConfig,
    log::Logger,
};

/// TCP listener
pub struct TcpListener {
    config: ListenerConfig,
    hosts: VetisHosts<Host>,
    signal: Option<watch::Sender<bool>>,
    inner: Option<tokio::net::TcpListener>,
    logger: Option<Logger<LogSender>>,
    handle: Option<JoinHandle<VetisResult<()>>>,
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
            && self
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
            hosts: VetisHosts::new(HashMap::new()),
            signal: None,
            inner: None,
            logger: None,
            handle: None,
        }
    }
}

impl vetis::listener::Listener for TcpListener {
    type RuntimeHost = Host;
    type Logger = Logger<LogSender>;

    fn add_host(&mut self, host: Arc<Self::RuntimeHost>) -> VetisResult<()> {
        // Add a host
        let hosts = self
            .hosts
            .pin_owned();
        let hostname = match self.config.port() {
            80 | 443 => host
                .hostname()
                .to_string(),
            _ => format!("{}:{}", host.hostname(), self.config().port()),
        };
        hosts.insert(hostname, host.clone());
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

    fn reassign_port(&mut self, port: u16) {
        self.config
            .reassign_port(port);
    }

    fn config(&self) -> &ListenerConfig {
        &self.config
    }

    async fn listen(&mut self) -> VetisResult<()> {
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
                .map_err(|e| VetisError::Bind(e.to_string()))?
        };

        let (sender, receiver) = watch::channel(false);
        let mut shut_signal = receiver.clone();
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
                    dispatcher.stop().await
                }
                res = dispatcher.run() => res
            }
        });

        self.signal = Some(sender);
        self.handle = Some(handle);

        Ok(())
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
        Ok(())
    }
}

struct ConnectionDispatcher {
    listener: tokio::net::TcpListener,
    hosts: VetisHosts<Host>,
    config: ListenerConfig,
    signal: Option<watch::Sender<bool>>,
    logger: Option<Logger<LogSender>>,
    workers: JoinSet<VetisResult<()>>,
    sender: Option<MAsyncTx<Array<TcpStream>>>,
}

unsafe impl Send for ConnectionDispatcher {}

/// Decompose the TCP listener into smaller, more manageable structs
impl ConnectionDispatcher {
    pub fn new(
        listener: tokio::net::TcpListener,
        hosts: VetisHosts<Host>,
        config: ListenerConfig,
        logger: Option<Logger<LogSender>>,
    ) -> Self {
        Self {
            listener,
            hosts,
            config,
            signal: None,
            logger,
            workers: JoinSet::new(),
            sender: None,
        }
    }

    async fn init(&mut self, receiver: MAsyncRx<Array<TcpStream>>) -> VetisResult<()> {
        info!(&self.logger, "Initializing tcp workers...");
        let (shut_sender, shut_recv) = watch::channel(false);
        let tls_config = TlsFactory::create_tls_config(self.hosts.clone()).await?;
        let tls_acceptor = TlsAcceptor::from(tls_config.clone());
        for worker_num in 1..=self
            .config
            .workers()
        {
            info!(&self.logger, "Initializing tcp worker: {}", worker_num);
            // TODO: Check ACL before proceeding
            let mut worker = TcpWorker::new(
                worker_num,
                tls_acceptor.clone(),
                self.hosts.clone(),
                self.logger.clone(),
                receiver.clone(),
            );

            let mut shut_signal = shut_recv.clone();
            let logger = self.logger.clone();
            let worker_future = async move {
                tokio::select! {
                    _ = shut_signal.changed() => {
                        info!(logger, "Stopping tcp worker {}...", worker.id());
                        worker.stop().await
                    },
                    res = worker.run() => res
                }
            };

            self.workers
                .spawn(worker_future);
        }

        self.signal = Some(shut_sender);

        Ok(())
    }

    async fn run(&mut self) -> VetisResult<()> {
        let (dispatch_sender, dispatch_recv) = mpmc::bounded_async::<TcpStream>(
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

        loop {
            let Ok((tcp_stream, _)) = self
                .listener
                .accept()
                .await
            else {
                debug!(
                    self.logger,
                    "Cannot accept connection: {:?}",
                    self.listener
                        .accept()
                        .await
                        .err()
                );

                continue;
            };

            if let Some(sender) = self.sender.as_ref()
                && let Err(e) = sender
                    .send(tcp_stream)
                    .await
            {
                error!(self.logger, "Could not distribute connection: {}", e.to_string())
            }
        }
    }

    // Initiate graceful shutdown and complete tasks
    pub async fn stop(mut self) -> VetisResult<()> {
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
                    info!(&self.logger, "TCP worker stopped successfully!");
                }
                Err(e) => {
                    error!(self.logger, "Internal error: {:?}", e);
                }
            }
        }

        Ok(())
    }
}
