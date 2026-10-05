use crate::{host::Host, service::http::HttpService};
use crossfire::{AsyncRxTrait, MAsyncRx, mpmc::Array};
use hyper_util::{
    rt::{TokioExecutor, TokioIo},
    server::conn::auto,
};
use papaya::HashMap;
use std::{net::SocketAddr, sync::Arc};
use tokio::{
    net::TcpStream,
    sync::watch,
    task::{self, JoinHandle},
};
use tokio_rustls::TlsAcceptor;
use vetis::{LogSender, VetisHosts, VetisResult, errors::VetisError, info, log::Logger};

pub(crate) struct TcpWorker {
    id: usize,
    acceptor: TlsAcceptor,
    hosts: VetisHosts<Host>,
    logger: Option<Logger<LogSender>>,
    receiver: MAsyncRx<Array<TcpStream>>,
    connections: Arc<HashMap<SocketAddr, JoinHandle<VetisResult<()>>>>,
    signal: Option<watch::Sender<bool>>,
}

unsafe impl Send for TcpWorker {}
unsafe impl Sync for TcpWorker {}

impl TcpWorker {
    pub(crate) fn new(
        id: usize,
        acceptor: TlsAcceptor,
        hosts: VetisHosts<Host>,
        logger: Option<Logger<LogSender>>,
        receiver: MAsyncRx<Array<TcpStream>>,
    ) -> Self {
        Self {
            id,
            acceptor,
            hosts,
            logger,
            receiver,
            connections: HashMap::new().into(),
            signal: None,
        }
    }

    pub(crate) fn id(&self) -> usize {
        self.id
    }

    #[allow(unused)]
    pub(crate) fn total_connections(&self) -> usize {
        self.connections
            .len()
    }

    pub(crate) async fn run(&mut self) -> VetisResult<()> {
        let tls_acceptor = self
            .acceptor
            .clone();

        let (shut_send, _) = watch::channel(false);
        self.signal = Some(shut_send.clone());
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

            let service = HttpService::new(self.hosts.clone(), client_addr, self.logger.clone());
            let is_tls = buf.starts_with(&[0x16, 0x03]);
            let handle = if is_tls {
                let tls_acceptor = tls_acceptor.clone();
                let logger = self.logger.clone();
                let conns = self
                    .connections
                    .clone();
                let mut shut_signal = shut_send.subscribe();
                tokio::spawn(async move {
                    let tls_stream = tls_acceptor
                        .accept(tcp_stream)
                        .await
                        .map_err(|e| VetisError::Worker(e.to_string()))?;

                    let builder = auto::Builder::new(TokioExecutor::new());
                    tokio::select! {
                        _ = shut_signal.changed() => {
                            info!(logger, "Closing connection {}...", &client_addr);
                            Ok(())
                        }
                        res = builder.serve_connection_with_upgrades(TokioIo::new(tls_stream), service) => {
                            conns.pin().remove(&client_addr);
                            res.map_err(|e| VetisError::Worker(e.to_string()))
                        }
                    }
                })
            } else {
                let logger = self.logger.clone();
                let conns = self
                    .connections
                    .clone();
                let mut shut_signal = shut_send.subscribe();
                task::spawn(async move {
                    let builder = auto::Builder::new(TokioExecutor::new());
                    tokio::select! {
                        _ = shut_signal.changed() => {
                            info!(logger, "Closing connection {}...", client_addr);
                            Ok(())
                        }
                        res = builder.serve_connection_with_upgrades(TokioIo::new(tcp_stream), service) => {
                            conns.pin().remove(&client_addr);
                            res.map_err(|e| VetisError::Worker(e.to_string()))
                        }
                    }
                })
            };
            self.connections
                .pin()
                .insert(client_addr, handle);
        }
        Ok(())
    }

    pub(crate) async fn stop(mut self) -> VetisResult<()> {
        if let Some(signal) = self.signal.take() {
            let _ = signal.send(true);
        }

        for handle in self
            .connections
            .pin()
            .values()
        {
            handle.abort();
        }

        Ok(())
    }
}
