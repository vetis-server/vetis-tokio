#[cfg(feature = "http3")]
use crate::listener::udp::UdpListener;
use crate::{
    host::Host,
    listener::{Listener, tcp::TcpListener},
    worker::log::LogWorker,
};
use crossfire::mpsc::{self};
use http::Version;
use log::info;
use std::{
    sync::Arc,
    thread::{self},
};
use vetis::{
    LogSender, VetisResult,
    errors::{ListenerError, VetisError},
    host::Host as _,
    listener::{Listener as _, ListenerConfig},
    log::{LogMessage, Logger},
    server::ServerConfig,
};

async fn add_host_to_listeners(
    host: Arc<Host>,
    listeners: &mut Vec<Listener>,
    workers: usize,
) -> VetisResult<()> {
    let host_config = host.config();

    // Check protos for this host
    let has_tcp_listener = host_config
        .protos()
        .contains(&Version::HTTP_11)
        || host_config
            .protos()
            .contains(&Version::HTTP_2);
    #[cfg(feature = "http3")]
    let has_udp_listener = host_config
        .protos()
        .contains(&Version::HTTP_3);

    // Bind hosts to listeners
    let host = host.clone();
    for bind_address in host_config
        .bind_addresses()
        .iter()
    {
        let listener_config = ListenerConfig::builder()
            .interface(bind_address.0)
            .port(bind_address.1)
            .workers(workers)
            .build()?;
        #[cfg(feature = "http3")]
        let mut port = 0;
        if has_tcp_listener {
            let mut listener = Listener::Tcp(TcpListener::new(listener_config.clone()));
            if listener
                .config()
                .port()
                == 0
            {
                listener
                    .reserve_port()
                    .await?;
                #[cfg(feature = "http3")]
                {
                    port = listener
                        .config()
                        .port();
                }
            }

            let elem = listeners
                .iter()
                .position(|l: &Listener| l == &listener);
            let host = host.clone();
            if let Some(index) = elem {
                listeners[index].add_host(host.into())?;
            } else {
                listener.add_host(host.into())?;
                listeners.push(listener);
            }
        }

        #[cfg(feature = "http3")]
        if has_udp_listener {
            let mut listener = Listener::Udp(UdpListener::new(listener_config.clone()));
            // UDP is asking for reserve port and not port has been assigned to TCP
            if port == 0
                && listener
                    .config()
                    .port()
                    == 0
            {
                listener
                    .reserve_port()
                    .await?;
            } else {
                listener.reassign_port(port);
            }

            let elem = listeners
                .iter()
                .position(|l| l == &listener);
            let host = host.clone();
            if let Some(index) = elem {
                listeners[index].add_host(host.into())?;
            } else {
                listener.add_host(host.into())?;
                listeners.push(listener);
            }
        }
    }

    Ok(())
}

/// Builder for a vetis instance
pub struct VetisBuilder {
    pub(crate) config: ServerConfig,
    pub(crate) listeners: Vec<Listener>,
}

impl VetisBuilder {
    /// Allow set server config
    ///
    /// You can either add a complete server configuration here including hosts,
    /// or only workers, log and log queue size.
    ///
    /// This method is useful to add extra config settings to server when dealing
    /// with HandlerPath instances which are not serializable.
    pub fn config(mut self, config: ServerConfig) -> Self {
        self.config = config;
        self
    }

    /// Adds a host to the server.
    ///
    /// Hosts allow you to handle multiple domains on a single server instance.
    /// Each host is identified by its domain name.
    ///
    /// # Arguments
    ///
    /// * `host` - A type implementing the `Host` trait
    pub async fn add_host(mut self, host: Host) -> VetisResult<Self> {
        let host = Arc::new(host);
        add_host_to_listeners(
            host,
            &mut self.listeners,
            self.config
                .workers(),
        )
        .await?;
        Ok(self)
    }

    /// Build a new vetis instance
    pub fn build(self) -> Vetis {
        Vetis { config: self.config, listeners: self.listeners, logger: None }
    }
}

#[derive(Default)]
/// Main server instance that manages hosts and listeners.
///
/// The `Vetis` struct is the core of the VeTiS server. It handles:
/// - Managing multiple hosts
/// - Coordinating server listeners
/// - Starting and stopping the server
/// - Signal handling for graceful shutdown
///
/// # Examples
///
/// ```rust,no_run
/// use vetis::{server::ServerConfig};
/// use vetis_tokio::{Vetis, VetisServer as _};
///
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
///     let config = ServerConfig::builder().build()?;
///     let mut server = Vetis::new(config);
///
///     // Add hosts...
///
///     server.run().await?;
///     Ok(())
/// }
/// ```
pub struct Vetis {
    config: ServerConfig,
    listeners: Vec<Listener>,
    logger: Option<thread::JoinHandle<VetisResult<()>>>,
}

impl Vetis {
    /// Creates a new `Vetis` server instance with the given configuration.
    ///
    /// # Arguments
    ///
    /// * `config` - Server configuration containing listeners and global settings
    pub async fn from_config(config: ServerConfig) -> VetisResult<Vetis> {
        let mut listeners = Vec::new();
        for host_config in config
            .hosts()
            .iter()
        {
            let host = Arc::new(Host::new(host_config.clone()).await?);
            add_host_to_listeners(host, &mut listeners, config.workers()).await?;
        }

        Ok(Vetis { config, listeners, logger: None })
    }

    /// Return all listeners managed by server
    pub fn listeners(&self) -> &Vec<Listener> {
        &self.listeners
    }

    /// Create a new vetis instance builder
    pub fn builder() -> VetisBuilder {
        VetisBuilder { listeners: Vec::new(), config: ServerConfig::default() }
    }

    fn start_logger(&mut self) -> VetisResult<LogSender> {
        info!(target: "vetis", "Starting logger...");
        let (log_sender, log_receiver) = mpsc::bounded_async_blocking::<LogMessage>(
            self.config
                .logger_queue_size(),
        );
        let log_worker = LogWorker::new(log_receiver);
        let logger_handle = thread::spawn(move || log_worker.run());
        self.logger = Some(logger_handle);
        Ok(log_sender)
    }

    async fn start_listeners(&mut self, log_sender: LogSender) -> VetisResult<()> {
        info!(target: "vetis", "Starting listeners...");
        for listener in self
            .listeners
            .iter_mut()
        {
            listener.logger(Logger::new(log_sender.clone()));
            listener
                .listen()
                .await?;
        }

        Ok(())
    }
}

impl From<Vec<Listener>> for Vetis {
    fn from(value: Vec<Listener>) -> Self {
        Vetis { config: ServerConfig::default(), listeners: value, logger: None }
    }
}

impl vetis::VetisServer for Vetis {
    /// Listener type
    type RuntimeListener = Listener;
    /// Host type
    type RuntimeHost = Host;

    /// Returns a reference to the server configuration.
    ///
    /// This method provides access to the listeners and global settings
    /// configured when the server was created.
    fn config(&self) -> &ServerConfig {
        &self.config
    }

    /// Starts the server and runs until interrupted.
    ///
    /// Please note signal handling should be handled by app code, not here
    /// since it is just a runtime specific crate, for now we only print
    /// listeners information to help on user access.
    async fn run(&mut self) -> VetisResult<()> {
        self.start().await?;

        let addresses = self
            .listeners
            .iter()
            .fold(String::new(), |mut acc, listener| {
                let net_proto = match listener {
                    Listener::Tcp(_) => "[TCP]",
                    #[cfg(feature = "http3")]
                    Listener::Udp(_) => "[UDP]",
                };
                acc.push_str(net_proto);
                acc.push(' ');
                acc.push_str(
                    &listener
                        .config()
                        .interface()
                        .to_string(),
                );
                acc.push(':');
                acc.push_str(
                    &listener
                        .config()
                        .port()
                        .to_string(),
                );
                acc.push(' ');
                acc
            });

        info!(target: "vetis", "Server listening on: {}", addresses);

        Ok(())
    }

    /// Starts the server without blocking.
    ///
    /// This method starts the server and returns immediately, allowing
    /// you to perform additional setup or handle shutdown manually.
    ///
    /// Listeners and workers will be started here.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - No hosts have been added
    /// - Server fails to bind to configured addresses
    /// - TLS configuration fails
    async fn start(&mut self) -> VetisResult<()> {
        if self
            .listeners
            .is_empty()
        {
            return Err(VetisError::Listener(ListenerError::NoListeners));
        }

        let log_sender = self.start_logger()?;

        self.start_listeners(log_sender)
            .await?;

        Ok(())
    }

    /// Stops the server gracefully.
    ///
    /// This method shuts down all listeners and waits for ongoing
    /// requests to complete before returning.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - No listeners available
    /// - Server fails to stop properly
    async fn stop(self) -> VetisResult<()> {
        if self
            .listeners
            .is_empty()
        {
            return Err(VetisError::Stop("Vetis is not running".to_string()));
        }

        for listener in self.listeners {
            listener
                .stop()
                .await?
        }
        Ok(())
    }
}
