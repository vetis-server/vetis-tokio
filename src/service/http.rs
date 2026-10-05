use crate::{
    Request,
    errors::{ContentError, HostError},
};
use http::{
    HeaderName, HeaderValue, StatusCode,
    header::{self},
};
use hyper::{body::Incoming, service::Service};
use hyper_body_utils::HttpBody;
use std::net::SocketAddr;
use vetis::{
    LogSender, VetisFutureResult, VetisHosts, debug, error, errors::VetisError, info, log::Logger,
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
                let version = req.version();
                let method = req.method().clone();
                let uri = req.uri().clone();

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

                // Make a vetis request and route to appropriate path
                let (parts, body) = req.into_parts();
                let request = Request::from_parts(parts, HttpBody::incoming(body));
                let vetis_response = host
                    .route(request, logger.clone())
                    .await?;

                // Add default headers
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

                let message = if let Some(query) = uri.query() {
                    format!(
                        "{} {} {}?{} {:?} {}",
                        client_addr,
                        method,
                        uri,
                        query,
                        version,
                        response.status()
                    )
                } else {
                    format!(
                        "{} {} {} {:?} {}",
                        client_addr,
                        method,
                        uri,
                        version,
                        response.status()
                    )
                };

                info!(logger, target: "vetis", "{}", message);
                info!(logger, target: &hostname, "{}", message);

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
