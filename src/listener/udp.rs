use crate::{host::Host, tls::TlsFactory, worker::udp::UdpWorker, VetisHosts};
use h3_quinn::quinn::{self, crypto::rustls::QuicServerConfig};
use log::{debug, error, info};
use papaya::HashMap;
use quinn::Endpoint;
use std::{net::SocketAddr, sync::Arc};
use tokio_util::sync::CancellationToken;
use vetis::{
    errors::{ListenerError, StartError, VetisError},
    host::Host as _,
    listener::ListenerConfig,
    Alpn, VetisResult,
};

/// UDP listener
pub struct UdpListener {
    config: ListenerConfig,
    pub(crate) hosts: VetisHosts<Host>,
    token: Option<CancellationToken>,
    inner: Option<Endpoint>,
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
        Self { config, hosts: VetisHosts::new(HashMap::new()), token: None, inner: None }
    }

    async fn create_inner_listener(&mut self) -> VetisResult<Endpoint> {
        let addr = SocketAddr::new(
            *self
                .config
                .interface(),
            self.config.port(),
        );

        let alpn_protos: Vec<Vec<u8>> = self
            .config
            .alpn_protos()
            .iter()
            .filter(|val| match val {
                Alpn::H3 | Alpn::Doq => true,
                _ => false,
            })
            .map(|val| val.into())
            .collect();

        let tls_config = TlsFactory::create_tls_config(self.hosts.clone(), alpn_protos).await?;
        let quic_config = QuicServerConfig::try_from(tls_config)
            .map_err(|e| VetisError::Start(StartError::Tls(e.to_string())))?;
        let server_config = quinn::ServerConfig::with_crypto(Arc::new(quic_config));

        quinn::Endpoint::server(server_config, addr)
            .map_err(|e| VetisError::Listener(ListenerError::Bind(e.to_string())))
    }
}

impl vetis::listener::Listener for UdpListener {
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

    fn config(&self) -> &ListenerConfig {
        &self.config
    }

    /// Listen for incoming connections
    ///
    /// # Returns
    ///
    /// * `ListenerResult<'_, ()>` - A `ListenerResult` instance containing the result of the listener.
    async fn listen(&mut self) -> VetisResult<()> {
        let listener = if let Some(listener) = self.inner.take() {
            listener
        } else {
            self.create_inner_listener()
                .await?
        };

        let dispatcher_token = CancellationToken::new();
        let token = dispatcher_token.clone();
        let mut dispatcher =
            ConnectionDispatcher::new(listener, self.hosts.clone(), dispatcher_token.child_token());
        tokio::spawn(async move {
            tokio::select! {
                _ = token.cancelled() => {
                    info!("Listener stopping");
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
    listener: quinn::Endpoint,
    hosts: VetisHosts<Host>,
    token: CancellationToken,
}

impl ConnectionDispatcher {
    pub fn new(
        listener: quinn::Endpoint,
        hosts: VetisHosts<Host>,
        token: CancellationToken,
    ) -> Self {
        Self { listener, hosts, token }
    }

    async fn dispatch_connections(&mut self) -> VetisResult<()> {
        while let Some(new_conn) = self
            .listener
            .accept()
            .await
        {
            let mut worker = UdpWorker::new(
                self.hosts.clone(),
                self.token
                    .child_token(),
            );
            let token = self.token.clone();
            let handle = tokio::spawn(async move {
                tokio::select! {
                    _ = token.cancelled() => {
                        debug!("Worker stopping..");
                    },
                    _ = worker.run(new_conn) => {}
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

        self.listener
            .wait_idle()
            .await;

        Ok(())
    }
}
