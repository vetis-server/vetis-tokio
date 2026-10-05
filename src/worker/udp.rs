use crate::{host::Host, service::quic::HttpService};
use bytes::Bytes;
use crossfire::{MAsyncRx, mpmc::Array};
use futures_util::StreamExt;
use h3::{
    ext::Protocol,
    server::{self, RequestStream},
};
use h3_quinn::Connection as QuinnConnection;
use http::{Method, Request};
use hyper::service::Service;
use hyper_body_utils::HttpBody;
use papaya::HashMap;
use std::{net::SocketAddr, sync::Arc};
use tokio::{
    sync::watch,
    task::{self, JoinHandle},
};
use vetis::{
    LogSender, VetisHosts, VetisResult, debug, error, errors::VetisError, info, log::Logger,
};

pub(crate) struct UdpWorker {
    id: usize,
    hosts: VetisHosts<Host>,
    logger: Option<Logger<LogSender>>,
    receiver: MAsyncRx<Array<quinn::Connection>>,
    connections: Arc<HashMap<SocketAddr, Option<JoinHandle<VetisResult<()>>>>>,
    signal: Option<watch::Sender<bool>>,
}

unsafe impl Send for UdpWorker {}
unsafe impl Sync for UdpWorker {}

impl UdpWorker {
    pub fn new(
        id: usize,
        hosts: VetisHosts<Host>,
        logger: Option<Logger<LogSender>>,
        receiver: MAsyncRx<Array<quinn::Connection>>,
    ) -> Self {
        Self { id, hosts, logger, receiver, connections: HashMap::new().into(), signal: None }
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
        let logger = self.logger.clone();
        let (shut_send, _) = watch::channel(false);
        self.signal = Some(shut_send.clone());
        info!(&self.logger, "UDP worker {} started", self.id);
        while let Ok(udp_stream) = self
            .receiver
            .recv()
            .await
        {
            let client_addr = udp_stream.remote_address();
            let service = HttpService::new(self.hosts.clone(), client_addr, self.logger.clone());
            let logger = logger.clone();
            let conns = self
                .connections
                .clone();
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
                        res.map_err(|e| VetisError::Worker(e.to_string()))
                    }
                }
            });
            self.connections
                .pin()
                .insert(client_addr, Some(handle));
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
            if let Some(handle) = handle {
                handle.abort();
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

        if response.is_err() {
            error!(
                logger,
                "Error serving connection: {:?}",
                response
                    .as_ref()
                    .err()
            );
            return Err(VetisError::Worker(
                response
                    .err()
                    .unwrap()
                    .to_string(),
            ));
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

        if response.is_err() {
            error!(
                logger,
                "Error serving connection: {:?}",
                response
                    .as_ref()
                    .err()
            );
            return Err(VetisError::Worker(
                response
                    .err()
                    .unwrap()
                    .to_string(),
            ));
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
                            .map_err(|e| VetisError::Worker(e.to_string()))?;
                    }
                }
            }

            send_stream
                .finish()
                .await
                .map_err(|e| VetisError::Worker(e.to_string()))?;
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
            .map_err(|e| VetisError::Worker(e.to_string()))?;

        while let Some(resolver) = h3_conn
            .accept()
            .await
            .map_err(|e| VetisError::Worker(e.to_string()))?
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
