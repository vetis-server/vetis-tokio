use crate::{
    Request,
    errors::{ContentError, HostError},
    host::Host,
};
use crossfire::{MAsyncRx, mpmc::Array};
use http::{HeaderName, HeaderValue, StatusCode, header};
use hyper::{body::Incoming, service::Service};
use hyper_body_utils::HttpBody;
use hyper_util::{
    rt::{TokioExecutor, TokioIo},
    server::conn::auto,
};
use std::net::SocketAddr;
use tokio::{
    net::TcpStream,
    task::{self},
};
use tokio_rustls::TlsAcceptor;
use vetis::{
    LogSender, VetisFutureResult, VetisHosts, VetisResult, debug, error, errors::VetisError, info,
    log::Logger,
};

// TODO: Add support to manage connections (hanging ones)

pub struct TcpWorker {
    id: usize,
    acceptor: TlsAcceptor,
    hosts: VetisHosts<Host>,
    logger: Option<Logger<LogSender>>,
    receiver: MAsyncRx<Array<TcpStream>>,
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
        Self { id, acceptor, hosts, logger, receiver }
    }

    pub fn id(&self) -> usize {
        self.id
    }

    pub async fn run(&mut self) -> VetisResult<()> {
        let tls_acceptor = self
            .acceptor
            .clone();
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
            if is_tls {
                let tls_acceptor = tls_acceptor.clone();
                let logger = self.logger.clone();
                tokio::spawn(async move {
                    let tls_stream = tls_acceptor
                        .accept(tcp_stream)
                        .await;
                    if let Err(e) = tls_stream {
                        error!(logger, "Could not accept connection: {}", e.to_string());
                        return;
                    }

                    let result = auto::Builder::new(TokioExecutor::new())
                        .serve_connection_with_upgrades(TokioIo::new(tls_stream.unwrap()), service)
                        .await;

                    if let Err(e) = result {
                        error!(logger, "Could serve request: {}", e.to_string());
                        return;
                    }
                });
            } else {
                let logger = self.logger.clone();
                task::spawn(async move {
                    let result = auto::Builder::new(TokioExecutor::new())
                        .serve_connection_with_upgrades(TokioIo::new(tcp_stream), service)
                        .await;

                    if let Err(e) = result {
                        error!(logger, "Could serve request: {}", e.to_string());
                        return;
                    }
                });
            }
        }
        Ok(())
    }

    pub async fn stop(&mut self) -> VetisResult<()> {
        Ok(())
    }
}

/// HttpService is responsible for process HTTP1 and HTTP2 client requests
pub struct HttpService<H> {
    hosts: VetisHosts<H>,
    client_addr: SocketAddr,
    logger: Option<Logger<LogSender>>,
}

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

impl<H> Service<http::request::Request<Incoming>> for HttpService<H>
where
    H: vetis::host::Host + Sync + Send + 'static,
{
    type Response = http::response::Response<HttpBody>;

    type Error = VetisError;

    type Future = VetisFutureResult<'static, Self::Response>;

    fn call(&self, req: http::request::Request<Incoming>) -> Self::Future {
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
                .or_else(|| {
                    req.headers()
                        .get(header::HOST)
                        .and_then(|h| h.to_str().ok())
                        .map(|h| h.to_string())
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
                let request = Request::from_parts(parts, HttpBody::from_incoming(body));

                let version = request
                    .version()
                    .clone();

                let method = request
                    .method()
                    .clone();

                let uri = request
                    .uri()
                    .clone();

                if let Some(scheme) = uri.scheme_str()
                    && (scheme.to_lowercase() == "http" || scheme.to_lowercase() == "ws")
                    && host
                        .config()
                        .allow_unsafe_connections()
                {
                    if host
                        .config()
                        .tls()
                        .is_some()
                        && host
                            .config()
                            .enable_hsts()
                    {
                        let target = if let Some(query) = uri.query() {
                            format!("{scheme}://{hostname}{}{}", uri.path(), query)
                        } else {
                            format!("{scheme}://{hostname}{}", uri.path())
                        };

                        // TODO: Handle HSTS via Strict-Transport-Security?

                        let header_value = HeaderValue::from_str(&target).map_err(|e| {
                            VetisError::Host(HostError::Content(ContentError::ServerError(
                                e.to_string(),
                            )))
                        })?;
                        let response = crate::Response::builder()
                            .status(StatusCode::PERMANENT_REDIRECT)
                            .header(header::LOCATION, header_value)
                            .text("Unprotected connection denied.")
                            .into_inner();
                        return Ok(response);
                    }
                }

                let vetis_response = host
                    .route(request, logger.clone())
                    .await?;

                let mut response = vetis_response.into_inner();

                let default_headers = host
                    .config()
                    .default_headers();

                if let Some(default_headers) = default_headers {
                    for (key, value) in default_headers.iter() {
                        let header_name = HeaderName::from_bytes(key.as_bytes()).map_err(|e| {
                            VetisError::Host(HostError::Content(ContentError::ServerError(
                                e.to_string(),
                            )))
                        })?;

                        let header_value = HeaderValue::from_str(value).map_err(|e| {
                            VetisError::Host(HostError::Content(ContentError::ServerError(
                                e.to_string(),
                            )))
                        })?;

                        response
                            .headers_mut()
                            .insert(header_name, header_value);
                    }
                }

                if let Some(query) = uri.query() {
                    info!(
                        logger,
                        target: &hostname,
                        "{} {} {}?{} {:?} {}",
                        client_addr,
                        method,
                        uri,
                        query,
                        version,
                        response.status()
                    );
                } else {
                    info!(
                        logger,
                        target: &hostname,
                        "{} {} {} {:?} {}",
                        client_addr,
                        method,
                        uri,
                        version,
                        response.status()
                    );
                }

                Ok::<http::Response<HttpBody>, VetisError>(response)
            } else {
                error!(logger, "Host not found: {}", hostname);
                let response = crate::Response::builder()
                    .status(StatusCode::BAD_GATEWAY)
                    .text("Host not found")
                    .into_inner();
                Ok(response)
            }
        };

        Box::pin(future)
    }
}
