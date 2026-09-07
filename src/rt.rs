use crate::{host::Host, listener::Listener};
use log::info;
use std::sync::Arc;
use vetis::{
    errors::{ListenerError, VetisError},
    host::Host as _,
    listener::Listener as _,
    server::ServerConfig,
    VetisResult,
};

/// Builder for a vetis instance
pub struct VetisBuilder {
    pub(crate) listeners: Vec<Listener>,
}

impl VetisBuilder {
    /// Adds listeners to the server.
    pub fn add_listeners(mut self, listeners: Vec<Listener>) -> VetisResult<Self> {
        self.listeners
            .extend(listeners);
        Ok(self)
    }

    /// Adds a host to the server.
    ///
    /// Hosts allow you to handle multiple domains on a single server instance.
    /// Each host is identified by its domain name.
    ///
    /// # Arguments
    ///
    /// * `host` - A type implementing the `Host` trait
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use http::StatusCode;
    /// use vetis::{
    ///     server::ServerConfig,
    ///     host::{path::Path, handler_fn, HostConfig},
    ///     VetisServer as _
    /// };
    /// use vetis_tokio::{
    ///     host::{Host, path::HandlerPath},
    ///     listener::build_listeners,
    ///     Vetis,
    /// }};
    ///
    /// let host_config = HostConfig::builder()
    ///     .hostname("example.com")
    ///     .bind_addresses(vec![(
    ///         "0.0.0.0"
    ///             .parse()
    ///             .unwrap(),
    ///         80,
    ///     )])
    ///     .build()?;
    ///
    /// let mut host = Host::new(host_config);
    ///
    /// let mut root_path = HandlerPath::builder()
    ///     .uri("/")
    ///     .handler(handler_fn(|request| async move {
    ///         let response = vetis::Response::builder()
    ///             .status(StatusCode::OK)
    ///             .text("Hello, World!");
    ///         Ok(response)
    ///     }))
    ///     .build()?;
    ///
    /// host.add_path(root_path);
    /// let server = Vetis::builder()
    ///     .add_listeners(build_listeners(ipv4))?
    ///     .add_host(host)?
    ///     .build();
    ///
    /// Ok::<(), vetis::errors::VetisError>(())
    /// ```
    pub fn add_host(mut self, host: Host) -> VetisResult<Self> {
        if self
            .listeners
            .is_empty()
        {
            return Err(VetisError::Listener(ListenerError::NoListeners));
        }

        let host = Arc::new(host);
        for bind_address in host
            .config()
            .bind_addresses()
        {
            let mut listeners = self
                .listeners
                .iter_mut()
                .filter(|listener| {
                    let config = listener.config();
                    *config.interface() == bind_address.0 && config.port() == bind_address.1
                });

            while let Some(listener) = listeners.next() {
                listener.add_host(host.clone())?;
            }
        }

        Ok(self)
    }

    /// Build a new vetis instance
    pub fn build(self) -> Vetis {
        Vetis { config: ServerConfig::default(), listeners: self.listeners }
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
/// use vetis::{server::ServerConfig, VetisServer as _};
/// use vetis_tokio::Vetis;
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
    pub(crate) listeners: Vec<Listener>,
}

impl Vetis {
    /// Creates a new `Vetis` server instance with the given configuration.
    ///
    /// # Arguments
    ///
    /// * `config` - Server configuration containing listeners and global settings
    ///
    /// # Examples
    ///
    /// ```rust, no_run
    /// use vetis::server::ServerConfig;
    /// use vetis_tokio::Vetis;
    ///
    /// let config = ServerConfig::builder().build()?;
    /// let server = Vetis::new(config);
    ///
    /// Ok::<(), vetis::errors::VetisError>(())
    /// ```
    pub fn new(config: ServerConfig) -> Vetis {
        Vetis { config, listeners: Vec::new() }
    }

    /// Create a new vetis instance builder
    pub fn builder() -> VetisBuilder {
        VetisBuilder { listeners: Vec::new() }
    }
}

impl From<Vec<Listener>> for Vetis {
    fn from(value: Vec<Listener>) -> Self {
        Vetis { config: ServerConfig::default(), listeners: value }
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

    /// Returns a mut reference to the server configuration.
    ///
    /// This method provides access to the listeners and global settings
    /// configured when the server was created, allowing them to
    /// be modified.
    fn config_mut(&mut self) -> &mut ServerConfig {
        &mut self.config
    }

    /// Starts the server and runs until interrupted.
    ///
    /// This method combines `start()` and graceful shutdown handling:
    /// 1. Starts the server with all configured hosts
    /// 2. Listens for shutdown signals (Ctrl+C on Tokio, SIGQUIT on Smol)
    /// 3. Stops the server gracefully
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - No hosts have been added
    /// - Server fails to start
    /// - Server fails to stop
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use vetis::{server::ServerConfig, VetisServer as _};
    /// use vetis_tokio::Vetis;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), Box<dyn std::error::Error>> {
    ///     let config = ServerConfig::builder().build()?;
    ///     let mut server = Vetis::new(config);
    ///
    ///     // Add virtual hosts...
    ///
    ///     server.run().await?; // Runs until Ctrl+C
    ///     Ok(())
    /// }
    /// ```
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

        info!("Server listening on: {}", addresses);

        let _ = tokio::signal::ctrl_c().await;

        info!("\nStopping server...");

        self.stop().await?;

        Ok(())
    }

    /// Starts the server without blocking.
    ///
    /// This method starts the server and returns immediately, allowing
    /// you to perform additional setup or handle shutdown manually.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - No hosts have been added
    /// - Server fails to bind to configured addresses
    /// - TLS configuration fails
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use vetis::{server::ServerConfig, VetisServer as _};
    /// use vetis_tokio::Vetis;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), Box<dyn std::error::Error>> {
    ///     let config = ServerConfig::builder().build()?;
    ///     let mut server = Vetis::new(config);
    ///
    ///     // Add hosts...
    ///
    ///     server.start().await?;
    ///
    ///     // Server is now running, do other work...
    ///
    ///     server.stop().await?;
    ///     Ok(())
    /// }
    /// ```
    async fn start(&mut self) -> VetisResult<()> {
        if self
            .listeners
            .is_empty()
        {
            return Err(VetisError::Listener(ListenerError::NoListeners));
        }

        for listener in &mut self.listeners {
            listener
                .listen()
                .await?;
        }
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
    /// - No server instance is running
    /// - Server fails to stop properly
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use vetis::{server::ServerConfig, VetisServer as _};
    /// use vetis_tokio::Vetis;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), Box<dyn std::error::Error>> {
    ///     let config = ServerConfig::builder().build()?;
    ///     let mut server = Vetis::new(config);
    ///
    ///     server.start().await?;
    ///     // Server running...
    ///     server.stop().await?;
    ///     Ok(())
    /// }
    /// ```
    async fn stop(&mut self) -> VetisResult<()> {
        if self
            .listeners
            .is_empty()
        {
            return Err(VetisError::Stop("Vetis is not running".to_string()));
        }

        for listener in &mut self.listeners {
            listener
                .stop()
                .await?
        }
        Ok(())
    }

    /// Reload the server configuration
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use vetis::VetisServer as _;
    /// use vetis_tokio::Vetis;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), Box<dyn std::error::Error>> {
    ///     let config = vetis::server::ServerConfig::builder().build()?;
    ///     let mut server = Vetis::new(config);
    ///
    ///     let changed_config = vetis::server::ServerConfig::builder().build()?;
    ///     server.reload(changed_config, vec![]).await;
    ///
    ///     Ok(())
    /// }
    /// ```
    async fn reload(&mut self, _new_config: ServerConfig) -> VetisResult<()> {
        Ok(())
    }
}
