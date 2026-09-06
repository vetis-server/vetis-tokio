use crate::{host::Host, listener::ListenerResult, tls::TlsFactory, VetisHosts};
use genswap::GenSwap;
use http::Version;
use hyper::server::conn::http1;
#[cfg(feature = "http2")]
use hyper::server::conn::http2;
use hyper_util::{
    rt::{TokioExecutor, TokioIo},
    server::conn::auto,
};
use log::error;
use peekable::tokio::AsyncPeekable;
use std::{borrow::Cow, collections::HashMap, net::SocketAddr, sync::Arc};
use tokio::task::JoinHandle;
use tokio_rustls::TlsAcceptor;
use vetis::{
    errors::VetisError, host::Host as _, listener::ListenerConfig, server::http::HttpService,
    VetisResult,
};

/// TCP listener
pub struct TcpListener {
    task: Option<JoinHandle<VetisResult<()>>>,
    config: ListenerConfig,
    pub(crate) hosts: VetisHosts<Host>,
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
        Self { task: None, config, hosts: VetisHosts::new(GenSwap::new(HashMap::new())) }
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
        self.hosts
            .rcu(|hosts| {
                let mut hosts = HashMap::clone(&hosts);
                hosts.insert(format!("{}:{}", host.hostname(), self.config().port()), host.clone());
                hosts
            });
        Ok(())
    }

    /// Add a new host
    ///
    /// # Arguments
    ///
    /// * `host` - A host instance.
    fn remove_host(&mut self, _hostname: &str) -> VetisResult<()> {
        todo!();
        Ok(())
    }

    fn total_hosts(&self) -> usize {
        let guard = self.hosts.reader();
        guard.cached().len()
    }

    fn config(&self) -> &ListenerConfig {
        &self.config
    }

    /// Listen for incoming connections
    ///
    /// # Returns
    ///
    /// * `ListenerResult<'_, ()>` - A `ListenerResult` instance containing the result of the listener.
    fn listen(&mut self) -> ListenerResult<'_, ()> {
        let future = async move {
            let addr = SocketAddr::new(
                *self
                    .config
                    .interface(),
                self.config.port(),
            );

            let listener = tokio::net::TcpListener::bind(addr)
                .await
                .map_err(|e| VetisError::Bind(e.to_string()))?;

            let task = self
                .handle_connections(listener, self.hosts.clone())
                .await?;

            self.task = Some(task);

            Ok(())
        };

        Box::pin(future)
    }

    /// Stop the listener
    ///
    /// # Returns
    ///
    /// * `ListenerResult<'_, ()>` - A `ListenerResult` instance containing the result of the listener.
    fn stop(&mut self) -> ListenerResult<'_, ()> {
        let future = async move {
            if let Some(task) = self.task.take() {
                task.abort();
            }
            Ok(())
        };

        Box::pin(future)
    }
}

/// Decompose the TCP listener into smaller, more manageable structs
impl TcpListener {
    async fn handle_connections(
        &mut self,
        listener: tokio::net::TcpListener,
        hosts: VetisHosts<Host>,
    ) -> Result<JoinHandle<VetisResult<()>>, VetisError> {
        // Limit supported alpns for TCP only
        let alpn_protocols = self
            .config
            .protos()
            .iter()
            .map(|v| match *v {
                Version::HTTP_11 | Version::HTTP_3 => b"http/1.1".to_vec(),
                Version::HTTP_2 => b"h2".to_vec(),
                _ => panic!("Unsupported protocol"),
            })
            .collect();
        let tls_config = TlsFactory::create_tls_config(hosts.clone(), alpn_protocols).await?;
        let tls_config = match tls_config {
            Some(config) => config,
            None => {
                error!("Missing TLS config");
                return Err(VetisError::Tls("Missing TLS config".to_string()));
            }
        };

        let allow_plain_connection = self
            .config
            .allow_unsafe_connections();

        let tls_acceptor: TlsAcceptor = TlsAcceptor::from(Arc::new(tls_config));
        let future = async move {
            loop {
                let result = listener
                    .accept()
                    .await;

                let (tcp_stream, client_addr) = match result {
                    Ok(conn_info) => conn_info,
                    Err(e) => {
                        error!("Cannot accept connection: {:?}", e);
                        continue;
                    }
                };

                // TODO: Check ACL before proceeding

                let mut peekable = AsyncPeekable::from(tcp_stream);
                let mut peeked = [0; 2];
                let result = peekable
                    .peek_exact(&mut peeked)
                    .await;

                if let Err(e) = result {
                    error!("Cannot peek connection: {:?}", e);
                    continue;
                }

                let is_tls = peeked.starts_with(&[0x16, 0x03]);
                if is_tls {
                    let tls_stream = tls_acceptor
                        .accept(peekable)
                        .await;

                    let tls_stream = match tls_stream {
                        Ok(tls_stream) => tls_stream,
                        Err(e) => {
                            error!("Cannot accept connection: {:?}", e);
                            continue;
                        }
                    };

                    let alpn = &tls_stream
                        .get_ref()
                        .1
                        .alpn_protocol();
                    if let Some(alpn_code) = alpn {
                        let Cow::Borrowed(alpn_code) = String::from_utf8_lossy(alpn_code) else {
                            error!("Cannot accept connection");
                            continue;
                        };

                        match alpn_code {
                            "http/1.1" => {
                                let service = HttpService::new(hosts.clone(), client_addr);
                                tokio::spawn(
                                    http1::Builder::new()
                                        .serve_connection(TokioIo::new(tls_stream), service),
                                );
                            }
                            #[cfg(feature = "http2")]
                            "h2" => {
                                let service = HttpService::new(hosts.clone(), client_addr);
                                tokio::spawn(
                                    http2::Builder::new(TokioExecutor::new())
                                        .serve_connection(TokioIo::new(tls_stream), service),
                                );
                            }
                            _ => {
                                error!("Unsupported protocol: {}", alpn_code);
                            }
                        }
                    } else {
                        let service = HttpService::new(hosts.clone(), client_addr);
                        tokio::spawn(async move {
                            let result = auto::Builder::new(TokioExecutor::new())
                                .serve_connection_with_upgrades(TokioIo::new(tls_stream), service)
                                .await;
                            match result {
                                Err(e) => {
                                    error!("Error while processing request: {}", e.to_string())
                                }
                                Ok(()) => {}
                            }
                        });
                    }
                } else if allow_plain_connection {
                    let service = HttpService::new(hosts.clone(), client_addr);
                    tokio::spawn(async move {
                        let result = auto::Builder::new(TokioExecutor::new())
                            .serve_connection_with_upgrades(TokioIo::new(peekable), service)
                            .await;
                        match result {
                            Err(e) => {
                                error!("Error while processing request: {}", e.to_string())
                            }
                            Ok(()) => {}
                        }
                    });
                }
            }
        };

        Ok(tokio::spawn(future))
    }
}
