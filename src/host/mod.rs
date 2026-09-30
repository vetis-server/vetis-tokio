//! Host module
//!
//! This module provides functionality for creating and managing hosts,
//! including path routing and request handling.
use futures_util::TryStreamExt;
use http::StatusCode;
use http_body_util::StreamBody;
use hyper::body::Frame;
use hyper_body_utils::HttpBody;
use std::{path::PathBuf, sync::Arc};
use tokio::fs::{self, File};
use vetis::{
    LogSender, Request, Response, VetisFutureResult, VetisPathRouter, VetisResult, debug, error,
    errors::{ConfigError, ContentError, HostError, VetisError},
    host::{HostConfig, HostContext, path::Path},
    log::Logger,
    security::Tls,
};

use crate::io::ReaderStream;

pub mod path;

/// Host type
pub struct Host {
    config: HostConfig,
    paths: VetisPathRouter<Box<dyn Path + Send + Sync>>,
    tls: Option<Tls>,
}

unsafe impl Send for Host {}
unsafe impl Sync for Host {}

impl Host {
    /// Create a new host
    ///
    /// # Arguments
    ///
    /// * `host_config` - A `HostConfig` instance containing the host configuration.
    ///
    /// # Returns
    ///
    /// * `VetisResult<Self>` - A new `Host` instance.
    pub async fn new(host_config: HostConfig) -> VetisResult<Self> {
        let tls = if let Some(security_config) = host_config.tls() {
            if let Some(root_dir) = host_config.root_directory() {
                let cert_path = if security_config
                    .cert_file()
                    .is_relative()
                {
                    PathBuf::from_iter([root_dir, security_config.cert_file()])
                } else {
                    security_config
                        .cert_file()
                        .to_path_buf()
                };

                if !cert_path.exists() {
                    return Err(VetisError::Config(ConfigError::Tls(
                        "Certificate not found!".into(),
                    )));
                }

                let cert = fs::read(cert_path)
                    .await
                    .map_err(|e| {
                        VetisError::Config(ConfigError::Tls(format!(
                            "Could not load certificate: {}",
                            e.to_string()
                        )))
                    })?;
                let key_path = if security_config
                    .cert_file()
                    .is_relative()
                {
                    PathBuf::from_iter([root_dir, security_config.key_file()])
                } else {
                    security_config
                        .key_file()
                        .to_path_buf()
                };

                if !key_path.exists() {
                    return Err(VetisError::Config(ConfigError::Tls("Key not found!".into())));
                }

                let key = fs::read(&key_path)
                    .await
                    .map_err(|e| {
                        VetisError::Config(ConfigError::Tls(format!(
                            "Could not load key: {}",
                            e.to_string()
                        )))
                    })?;
                Some(Tls::from_cert_and_key(&cert, &key))
            } else {
                if !security_config
                    .cert_file()
                    .exists()
                {
                    return Err(VetisError::Config(ConfigError::Tls(
                        "Certificate not found!".into(),
                    )));
                }

                let cert = fs::read(security_config.cert_file())
                    .await
                    .map_err(|e| {
                        VetisError::Config(ConfigError::Tls(format!(
                            "Could not load certificate: {}",
                            e.to_string()
                        )))
                    })?;

                if !security_config
                    .key_file()
                    .exists()
                {
                    return Err(VetisError::Config(ConfigError::Tls("Key not found!".into())));
                }

                let key = fs::read(security_config.key_file())
                    .await
                    .map_err(|e| {
                        VetisError::Config(ConfigError::Tls(format!(
                            "Could not load key: {}",
                            e.to_string()
                        )))
                    })?;

                Some(Tls::from_cert_and_key(&cert, &key))
            }
        } else {
            None
        };

        let mut paths = VetisPathRouter::new();
        for path_config in host_config
            .paths()
            .iter()
        {
            let path = path_config.boxed_sync();
            paths.insert(
                path.uri()
                    .to_owned(),
                Arc::new(path),
            );
        }

        Ok(Self { config: host_config, paths, tls })
    }

    /// Add a path to the host
    ///
    /// # Arguments
    ///
    /// * `path` - A `HostPath` instance containing the path configuration.
    pub fn add_path<P>(&mut self, path: P)
    where
        P: Path + Send + Sync + 'static,
    {
        self.paths.insert(
            path.uri()
                .to_string(),
            Arc::new(Box::new(path)),
        );
    }

    /// Add tls
    ///
    /// # Arguments
    ///
    /// * `tls` - A `Tls` instance containing certicates.
    pub fn add_tls(&mut self, tls: Tls) {
        self.tls = Some(tls);
    }

    /// Returns security info about host
    ///
    /// # Returns
    ///
    /// * `Option<Tls>` - Some or None if TLS is set or not
    pub fn tls(&self) -> &Option<Tls> {
        &self.tls
    }
}

impl vetis::host::Host for Host {
    type Path = Box<dyn Path + Send + Sync>;

    fn paths(&self) -> &VetisPathRouter<Self::Path> {
        &self.paths
    }

    fn config(&self) -> &HostConfig {
        &self.config
    }

    fn config_mut(&mut self) -> &mut HostConfig {
        &mut self.config
    }

    fn serve_status_page<'a>(
        &'a self,
        status: u16,
        logger: Option<Logger<LogSender>>,
    ) -> VetisFutureResult<'a, Response> {
        let future = async move {
            let status_code = match StatusCode::from_u16(status) {
                Ok(code) => code,
                Err(_) => {
                    return Err(VetisError::Host(HostError::Content(ContentError::ServerError(
                        "Invalid status code".to_string(),
                    ))));
                }
            };

            let static_status_response = Response::builder()
                .status(status_code)
                .text(
                    status_code
                        .canonical_reason()
                        .unwrap_or("Unknown status code"),
                );

            if let Some(status_pages) = &self
                .config
                .status_pages()
            {
                if let Some(page) = status_pages.get(&status) {
                    if let Some(dir) = self
                        .config
                        .root_directory()
                    {
                        let file = dir.join(page.to_string());
                        if file.exists() {
                            let result = File::open(file).await;
                            if let Ok(data) = result {
                                let content = ReaderStream::new(data).map_ok(Frame::data);
                                let body = StreamBody::new(content);
                                return Ok(Response::builder()
                                    .status(status_code)
                                    .body(HttpBody::from_generic_stream(body)));
                            }
                        } else {
                            error!(logger, target: self.config.hostname(), "Could not find status page!");
                        }
                    }
                }
            } else {
                error!(logger, target: self.config.hostname(), "No configured status pages!");
            }
            Ok(static_status_response)
        };
        Box::pin(future)
    }

    /// Route request to the appropriate handler
    ///
    /// # Arguments
    ///
    /// * `request` - A `Request` instance containing the request information.
    ///
    /// # Returns
    ///
    /// * `Pin<Box<dyn Future<Output = Result<Response, VetisError>> + Send>>` - A pinned box
    ///    containing the future that will resolve to a `Result<Response, VetisError>`.
    fn route<'a>(
        &'a self,
        request: Request,
        logger: Option<Logger<LogSender>>,
    ) -> VetisFutureResult<'a, Response> {
        let uri_path: String = request
            .uri()
            .path()
            .into();

        if uri_path.starts_with("..") {
            return Box::pin(async move {
                debug!(logger, "Invalid access!");
                self.serve_status_page(http::StatusCode::FORBIDDEN.as_u16(), logger)
                    .await
            });
        }

        let paths = self.paths();
        let matches = paths.get(&uri_path);
        let path = if let Some(path) = matches {
            path
        } else {
            let matches = paths.get_longest_common_prefix(&uri_path);
            let Some(path) = matches else {
                return Box::pin(async move {
                    debug!(logger, "Could not find host path!");
                    self.serve_status_page(http::StatusCode::NOT_FOUND.as_u16(), logger)
                        .await
                });
            };
            path.1
        };

        let path = path.clone();
        let target_path = uri_path
            .strip_prefix(path.uri())
            .unwrap_or(&uri_path);

        let host_context = HostContext::from_uri(target_path)
            .with_logger(logger.clone())
            .with_root_directory(
                self.config
                    .root_directory()
                    .clone(),
            );

        let future = async move {
            let result = path.handle(request, host_context);
            match result.await {
                Ok(response) => Ok(response),
                Err(error) => {
                    match error {
                        VetisError::Host(HostError::Proxy(_)) => {
                            error!(logger, "{}", error);
                            return self
                                .serve_status_page(http::StatusCode::BAD_GATEWAY.as_u16(), logger)
                                .await;
                        }
                        VetisError::Host(HostError::Auth(_)) => {
                            error!(logger, "{}", error);
                            return self
                                .serve_status_page(http::StatusCode::UNAUTHORIZED.as_u16(), logger)
                                .await;
                        }
                        VetisError::Host(HostError::Content(inner_error)) => {
                            error!(logger, "{}", inner_error);
                            let status_code = match inner_error {
                                ContentError::Forbidden => StatusCode::FORBIDDEN.as_u16(),
                                ContentError::NotFound(_) => StatusCode::NOT_FOUND.as_u16(),
                                ContentError::InvalidMetadata(_) => {
                                    StatusCode::INTERNAL_SERVER_ERROR.as_u16()
                                }
                                ContentError::InvalidRange(_) => {
                                    StatusCode::RANGE_NOT_SATISFIABLE.as_u16()
                                }
                                ContentError::ServerError(_) => {
                                    StatusCode::INTERNAL_SERVER_ERROR.as_u16()
                                }
                            };
                            return self
                                .serve_status_page(status_code, logger)
                                .await;
                        }
                        _ => {}
                    }

                    Err(error)
                }
            }
        };

        Box::pin(future)
    }
}
