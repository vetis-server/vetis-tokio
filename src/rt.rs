use crate::{
    host::Host,
    listener::{Listener, tcp::TcpListener},
};
use http::Version;
use log::{error, info};
use papaya::HashMap;
use std::{collections::HashSet, sync::Arc};
use tokio::{sync::watch, task::JoinSet};
use vetis::{
    VetisResult,
    errors::{HostError, VetisError},
    host::HostConfig,
    listener::{Listener as _, ListenerConfig},
    server::ServerConfig,
};

/// Builder for a vetis instance
pub struct VetisBuilder {
    pub(crate) config: ServerConfig,
}

impl VetisBuilder {
    /// Allow set server config
    ///
    /// You can either add a complete server configuration.
    ///
    /// This method is useful to add extra config settings to server.
    ///
    /// # Arguments
    ///
    /// * `config` - Server configuration
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
    /// * `host` - A type implementing the `HostConfig` trait
    pub fn add_host(mut self, host: HostConfig) -> Self {
        self.config
            .hosts_mut()
            .push(host);
        self
    }

    /// Build a new vetis instance
    pub fn build(self) -> Vetis {
        Vetis { config: self.config, listeners: JoinSet::new(), signal: None }
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
///     let mut server = Vetis::from_config(config).await?;
///
///     // Add hosts...
///
///     server.run().await?;
///     Ok(())
/// }
/// ```
pub struct Vetis {
    config: ServerConfig,
    listeners: JoinSet<VetisResult<()>>,
    signal: Option<watch::Sender<bool>>,
}

impl Vetis {
    /// Creates a new `Vetis` server instance with the given configuration.
    ///
    /// # Arguments
    ///
    /// * `config` - Server configuration containing listeners and global settings
    pub async fn from_config(config: ServerConfig) -> VetisResult<Vetis> {
        Ok(Vetis { config, listeners: JoinSet::new(), signal: None })
    }

    /// Return all listeners managed by server
    pub fn listeners(&self) -> &JoinSet<VetisResult<()>> {
        &self.listeners
    }

    /// Create a new vetis instance builder
    pub fn builder() -> VetisBuilder {
        VetisBuilder { config: ServerConfig::default() }
    }

    async fn start_cache(&mut self) -> VetisResult<()> {
        info!(target: "vetis", "Starting cache...");

        Ok(())
    }

    async fn start_listeners(&mut self) -> VetisResult<()> {
        // Build and start listeners based on hosts configuration
        // Each host can bind to one of most listeners.
        // Here we build a registry of hosts and pass this registry
        // to all listeners available.

        let mut listeners = HashSet::new();
        let hosts = HashMap::new();
        for host_config in self.config.hosts() {
            // Check if HTTP/1.1 or HTTP/2 are enabled for this server
            let has_tcp_listener = host_config
                .protos()
                .contains(&Version::HTTP_11)
                || host_config
                    .protos()
                    .contains(&Version::HTTP_2);

            // Check if HTTP/2 is enabled for this server
            #[cfg(feature = "http3")]
            let has_udp_listener = host_config
                .protos()
                .contains(&Version::HTTP_3);

            let host = Arc::new(Host::new(host_config.clone()).await?);

            // Bind hosts to listeners
            for (ip, port) in host_config
                .bind_addresses()
                .iter()
            {
                let listener_config = ListenerConfig::builder()
                    .interface(*ip)
                    .port(*port)
                    .build()?;

                #[cfg(feature = "http3")]
                // That will allow to "borrow" port from TCP
                let mut port = 0;

                if has_tcp_listener {
                    let mut listener = Listener::Tcp(TcpListener::new(listener_config.clone()));
                    if listener
                        .config()
                        .port()
                        == 0
                    {
                        // If port is set to 0 on config, that means we should ask OS for a port
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
                    listeners.insert(listener);
                }

                #[cfg(feature = "http3")]
                if has_udp_listener {
                    let mut listener = Listener::Udp(UdpListener::new(listener_config.clone()));
                    // UDP is asking for reserve port and not for port which has been assigned to TCP
                    if port == 0
                        && listener
                            .config()
                            .port()
                            == 0
                    {
                        // If port is set to 0 on config, that means we should ask OS for a port
                        listener
                            .reserve_port()
                            .await?;
                    // Got an port assigned to TCP listener, use same on UDP listener
                    } else if port > 0 {
                        listener.reassign_port(port);
                    }

                    listeners.insert(listener);
                }

                // Normalize hostname based on default ports
                let hostname = match port {
                    80 | 443 => host_config
                        .hostname()
                        .to_string(),
                    _ => format!("{}:{}", host_config.hostname(), port),
                };

                hosts
                    .pin()
                    .insert(hostname, host.clone());
            }
        }

        info!(target: "vetis", "Starting listeners...");
        let (shut_sender, shut_recv) = watch::channel(false);
        let hosts = Arc::new(hosts);
        let mut addresses = String::new();
        for mut listener in listeners {
            let mut shut_signal = shut_recv.clone();

            match &listener {
                Listener::Tcp(tcp) => addresses.push_str(&format!(
                    "[TCP] {}:{}",
                    tcp.config()
                        .interface(),
                    tcp.config().port()
                )),
                #[cfg(feature = "http3")]
                Listener::Udp(_) => addresses.push_str(&format!(
                    "[UDP] {}:{}",
                    tcp.config()
                        .interface(),
                    tcp.config().port()
                )),
            }

            let hosts = hosts.clone();
            let listener_future = async move {
                // Ctrl + C or similar handling logic
                tokio::select! {
                    _ = shut_signal.changed() => {
                        // Perform a graceful shutdown
                        listener.stop().await
                    },
                    res = listener.listen(hosts) => res
                }
            };

            // Start a listener and keep an handle for graceful shutdown
            self.listeners
                .spawn(listener_future);
        }

        info!(target: "vetis", "Server listening on {addresses}");

        self.signal = Some(shut_sender);

        Ok(())
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
            .config
            .hosts()
            .is_empty()
        {
            return Err(VetisError::Host(HostError::NoHosts));
        }

        self.start_cache()
            .await?;

        self.start_listeners()
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
    async fn stop(mut self) -> VetisResult<()> {
        if self
            .listeners
            .is_empty()
        {
            return Err(VetisError::Stop("Vetis is not running".into()));
        }

        if let Some(signal) = self.signal.take() {
            let _ = signal.send(true);
        }

        while let Some(handle) = self
            .listeners
            .join_next()
            .await
        {
            match handle {
                Ok(_) => {
                    info!(target: "vetis", "Listener stopped successfully!");
                }
                Err(e) => {
                    error!(target: "vetis", "Internal error: {:?}", e);
                }
            }
        }

        Ok(())
    }
}
