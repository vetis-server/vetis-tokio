use crate::{host::Host, service::http::HttpService};
use crossfire::{MAsyncRx, mpmc::Array};
use hyper_util::{
    rt::{TokioExecutor, TokioIo},
    server::conn::auto,
};
use std::{collections::HashMap, net::SocketAddr};
use tokio::{
    net::TcpStream,
    sync::watch,
    task::{self, JoinHandle},
};
use tokio_rustls::TlsAcceptor;
use vetis::{LogSender, VetisHosts, VetisResult, error, errors::VetisError, info, log::Logger};

// TODO: Add support to manage connections (hanging ones)

pub(crate) struct TcpWorker {
    id: usize,
    acceptor: TlsAcceptor,
    hosts: VetisHosts<Host>,
    signal: Option<watch::Sender<bool>>,
    logger: Option<Logger<LogSender>>,
    receiver: MAsyncRx<Array<TcpStream>>,
    connections: HashMap<SocketAddr, JoinHandle<VetisResult<()>>>,
}

unsafe impl Send for TcpWorker {}
unsafe impl Sync for TcpWorker {}

impl TcpWorker {
    pub fn new(
        id: usize,
        acceptor: TlsAcceptor,
        hosts: VetisHosts<Host>,
        logger: Option<Logger<LogSender>>,
        receiver: MAsyncRx<Array<TcpStream>>,
    ) -> Self {
        Self { id, acceptor, hosts, signal: None, logger, receiver, connections: HashMap::new() }
    }

    pub fn id(&self) -> usize {
        self.id
    }

    pub async fn run(&mut self) -> VetisResult<()> {
        let (shut_sender, shut_recv) = watch::channel(false);
        let tls_acceptor = self
            .acceptor
            .clone();
        self.signal = Some(shut_sender);

        info!(&self.logger, "TCP worker {} started!", self.id);
        while let Ok(tcp_stream) = self
            .receiver
            .recv()
            .await
        {
            let Ok(client_addr) = tcp_stream.peer_addr() else {
                return Err(VetisError::Worker("Unkown client address".into()));
            };

            let mut buf = [0; 2];
            tcp_stream
                .peek(&mut buf)
                .await
                .map_err(|e| VetisError::Worker(e.to_string()))?;

            let mut shut_signal = shut_recv.clone();
            let service = HttpService::new(self.hosts.clone(), client_addr, self.logger.clone());
            let is_tls = buf.starts_with(&[0x16, 0x03]);
            if is_tls {
                let tls_acceptor = tls_acceptor.clone();
                let logger = self.logger.clone();
                let handle = tokio::spawn(async move {
                    let tls_stream = tls_acceptor
                        .accept(tcp_stream)
                        .await
                        .map_err(|e| VetisError::Worker(e.to_string()))?;

                    // Allow graceful shutdown of http service, also allow kick off service,
                    // but mind of trigger oneshot to remove entry from hashmap
                    let builder = auto::Builder::new(TokioExecutor::new());
                    tokio::select! {
                        _ = shut_signal.changed() => {
                            info!(logger, "Closing connection {}...", client_addr);
                            Ok(())
                        }
                        result = builder.serve_connection_with_upgrades(TokioIo::new(tls_stream), service) => {
                            match result {
                                Ok(_) => {
                                    Ok(())
                                }
                                Err(e) => {
                                    error!(logger, "Could serve request: {}", e.to_string());
                                    Err(VetisError::Worker(e.to_string()))
                                }
                            }
                        }
                    }
                });

                // Save handle to be able to kick connnections
                self.connections
                    .insert(client_addr, handle);
            } else {
                let logger = self.logger.clone();
                let handle = task::spawn(async move {
                    let builder = auto::Builder::new(TokioExecutor::new());
                    tokio::select! {
                        _ = shut_signal.changed() => {
                            info!(logger, "Closing connection {}...", client_addr);
                            Ok(())
                        }
                        result = builder.serve_connection_with_upgrades(TokioIo::new(tcp_stream), service) => {
                            match result {
                                Ok(_) => {
                                    Ok(())
                                }
                                Err(e) => {
                                    error!(logger, "Could serve request: {}", e.to_string());
                                    Err(VetisError::Worker(e.to_string()))
                                }
                            }
                        }
                    }
                });

                // Save handle to be able to kick connnections
                self.connections
                    .insert(client_addr, handle);
            }
        }
        Ok(())
    }

    pub async fn stop(mut self) -> VetisResult<()> {
        if let Some(signal) = self.signal.take() {
            let _ = signal.send(true);
        }
        for entry in self
            .connections
            .into_iter()
        {
            match entry.1.await {
                Ok(_) => {
                    info!(&self.logger, "HTTP client connection dropped!");
                }
                Err(e) => {
                    error!(self.logger, "Internal error: {:?}", e);
                }
            }
        }

        Ok(())
    }
}
