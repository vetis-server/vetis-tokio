use crate::host::Host;
use crossfire::{MAsyncRx, mpmc::Array};
use futures_util::StreamExt;
use h3::server::Connection;
use h3_quinn::Connection as QuinnConnection;
use http::{HeaderName, HeaderValue, StatusCode};
use hyper::service::Service;
use hyper_body_utils::HttpBody;
use std::net::SocketAddr;
use tokio::task::{self};
use vetis::{
    LogSender, Request, Response, VetisFutureResult, VetisHosts, VetisResult, debug, error,
    errors::VetisError, info, log::Logger,
};

pub struct UdpWorker {
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

struct Builder {}

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

/// HttpService is responsible for process HTTP1 and HTTP2 client requests
struct HttpService<H> {
    hosts: VetisHosts<H>,
    client_addr: SocketAddr,
    logger: Option<Logger<LogSender>>,
}

unsafe impl<H> Send for HttpService<H> {}
unsafe impl<H> Sync for HttpService<H> {}

impl<H> HttpService<H>
where
    H: vetis::host::Host,
{
    /// Create a new HttpService
    pub fn new(
        hosts: VetisHosts<H>,
        client_addr: SocketAddr,
        logger: Option<Logger<LogSender>>,
    ) -> Self {
        HttpService { hosts: hosts.clone(), client_addr, logger }
    }
}

impl<H> Service<http::request::Request<HttpBody>> for HttpService<H>
where
    H: vetis::host::Host + Sync + Send + 'static,
{
    type Response = http::response::Response<HttpBody>;

    type Error = VetisError;

    type Future = VetisFutureResult<'static, Self::Response>;

    fn call(&self, req: http::request::Request<HttpBody>) -> Self::Future {
        let hosts = self.hosts.clone();
        let logger = self.logger.clone();
        let client_addr = self
            .client_addr
            .clone();
        let future = async move {
            let Some(hostname) = req
                .uri()
                .authority()
                .map(|a| {
                    a.as_str()
                        .to_string()
                })
            else {
                error!(logger, "No hostname found in request");
                let response = crate::Response::builder()
                    .status(StatusCode::BAD_REQUEST)
                    .text("No hostname found in request")
                    .into_inner();
                return Ok(response);
            };

            debug!(logger, "Serving request for host: {}", hostname);
            let hosts = hosts.pin_owned();
            let host = hosts.get(&hostname);
            if let Some(host) = host {
                let (parts, body) = req.into_parts();
                let request = Request::from_parts(parts, body);

                let method = request
                    .method()
                    .clone();

                let uri = request
                    .uri()
                    .path()
                    .to_string();

                let vetis_response = host
                    .route(request, logger.clone())
                    .await;

                let response = if let Err(err) = vetis_response {
                    error!(logger, "Error executing request: {:?}", err);
                    Response::builder()
                        .status(StatusCode::INTERNAL_SERVER_ERROR)
                        .text("Internal server error")
                        .into_inner()
                } else {
                    let mut response = vetis_response
                        .unwrap()
                        .into_inner();

                    let default_headers = host
                        .config()
                        .default_headers();

                    if let Some(default_headers) = default_headers {
                        for (key, value) in default_headers.iter() {
                            let Ok(header_name) = HeaderName::from_bytes(key.as_bytes()) else {
                                error!(logger, "Invalid header name: {}", key);
                                continue;
                            };

                            let Ok(header_value) = HeaderValue::from_str(value) else {
                                error!(logger, "Invalid header value: {}", value);
                                continue;
                            };

                            response
                                .headers_mut()
                                .insert(header_name, header_value);
                        }
                    }

                    response
                };

                info!(logger, "{} {} {} {}", client_addr, method, uri, response.status());

                Ok::<_, VetisError>(response)
            } else {
                error!(logger, "Host not found in request");
                let response = Response::builder()
                    .status(StatusCode::BAD_REQUEST)
                    .text("Host not found")
                    .into_inner();
                Ok(response)
            }
        };

        Box::pin(future)
    }
}
