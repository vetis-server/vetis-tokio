use http::{HeaderName, HeaderValue, StatusCode};
use hyper::service::Service;
use hyper_body_utils::HttpBody;
use std::net::SocketAddr;
use vetis::{
    LogSender, Request, Response, VetisFutureResult, VetisHosts, debug, error, errors::VetisError,
    info, log::Logger,
};

/// HttpService is responsible for process HTTP1 and HTTP2 client requests
pub(crate) struct HttpService<H> {
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
