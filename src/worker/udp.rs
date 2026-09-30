use crate::{host::Host, service::quic::HttpService};
use crossfire::{MAsyncRx, mpmc::Array};
use futures_util::StreamExt;
use h3::server::Connection;
use h3_quinn::Connection as QuinnConnection;
use hyper::service::Service;
use hyper_body_utils::HttpBody;
use tokio::task::{self};
use vetis::{
    LogSender, VetisHosts, VetisResult, debug, error, errors::VetisError, info, log::Logger,
};

pub(crate) struct UdpWorker {
    id: usize,
    hosts: VetisHosts<Host>,
    logger: Option<Logger<LogSender>>,
    receiver: MAsyncRx<Array<quinn::Connection>>,
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
        Self { id, hosts, logger, receiver }
    }

    pub fn id(&self) -> usize {
        self.id
    }

    pub async fn run(&self) -> VetisResult<()> {
        let logger = self.logger.clone();
        info!(&self.logger, "UDP worker {} started", self.id);
        while let Ok(udp_stream) = self
            .receiver
            .recv()
            .await
        {
            let client_addr = udp_stream.remote_address();
            let service = HttpService::new(self.hosts.clone(), client_addr, self.logger.clone());
            let logger = logger.clone();
            task::spawn(async move {
                Builder::new()
                    .serve_connection(udp_stream, service, logger)
                    .await
            });
        }

        Ok(())
    }

    pub async fn stop(&mut self) -> VetisResult<()> {
        Ok(())
    }
}

pub(crate) struct Builder {}

impl Builder {
    pub fn new() -> Self {
        Self {}
    }

    async fn serve_connection(
        &self,
        stream: quinn::Connection,
        service: HttpService<Host>,
        logger: Option<Logger<LogSender>>,
    ) -> VetisResult<()> {
        let mut h3_conn = Connection::new(QuinnConnection::new(stream))
            .await
            .map_err(|e| VetisError::Worker(e.to_string()))?;

        loop {
            let resolver = h3_conn
                .accept()
                .await
                .map_err(|e| VetisError::Worker(e.to_string()))?;

            if let Some(resolver) = resolver {
                let result = resolver
                    .resolve_request()
                    .await;

                if let Ok((req, stream)) = result {
                    let (mut send_stream, recv_stream) = stream.split();
                    let (parts, _) = req.into_parts();
                    let body = HttpBody::from_generic_server(recv_stream);
                    let request = http::Request::from_parts(parts, body);

                    let response = service
                        .call(request)
                        .await;

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
                                debug!(logger, "Successfully respond to connection");
                            }
                            Err(err) => {
                                error!(logger, "Unable to send response to connection: {:?}", err);
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
                    } else {
                        error!(
                            logger,
                            "HttpServer - Error serving connection: {:?}",
                            response.err()
                        );
                    }
                }
            } else {
                break Ok(());
            }
        }
    }
}
