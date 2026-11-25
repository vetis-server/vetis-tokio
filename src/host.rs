//! Host module
//!
//! This module provides functionality for creating and managing hosts,
//! including path routing and request handling.
use crate::io::ReaderStream;
use cel::{Context, Env};
use compact_str::{ToCompactString, format_compact};
use futures_util::TryStreamExt;
use http::StatusCode;
use http_body_util::StreamBody;
use hyper::body::Frame;
use hyper_body_utils::HttpBody;
use std::{path::PathBuf, sync::Arc};
use tokio::fs::{self, File};
use vetis::{
    LogSender, Request, Response, VetisFutureResult, VetisHook, VetisHooks, VetisResult, debug,
    error,
    errors::{ConfigError, HostError, RuntimeError, VetisError},
    hook::HookPhase,
    host::HostConfig,
    log::Logger,
    security::{Tls, TlsConfig},
};
use vetis_matchers::expr::extensions;

/// Host type
pub struct Host {
    config: HostConfig,
    hooks: VetisHooks,
    tls: Option<Tls>,
    env: Arc<Env>,
}

unsafe impl Send for Host {}
unsafe impl Sync for Host {}

async fn build_tls(
    tls_config: &TlsConfig,
    root_directory: Option<&PathBuf>,
) -> VetisResult<Option<Tls>> {
    if let Some(root_dir) = root_directory {
        let cert_path = if tls_config
            .cert_file()
            .is_relative()
        {
            PathBuf::from_iter([root_dir, tls_config.cert_file()])
        } else {
            tls_config
                .cert_file()
                .to_path_buf()
        };

        if !cert_path.exists() {
            return Err(VetisError::Config(ConfigError::tls("Certificate not found!")));
        }

        let cert = fs::read(cert_path)
            .await
            .map_err(|e| {
                VetisError::Config(ConfigError::tls(format_compact!(
                    "Could not load certificate: {}",
                    e.to_string()
                )))
            })?;
        let key_path = if tls_config
            .key_file()
            .is_relative()
        {
            PathBuf::from_iter([root_dir, tls_config.key_file()])
        } else {
            tls_config
                .key_file()
                .to_path_buf()
        };

        if !key_path.exists() {
            return Err(VetisError::Config(ConfigError::tls("Key not found!")));
        }

        let key = fs::read(&key_path)
            .await
            .map_err(|e| {
                VetisError::Config(ConfigError::tls(format_compact!(
                    "Could not load key: {}",
                    e.to_string()
                )))
            })?;

        Ok(Some(Tls::from_cert_and_key(&cert, &key)))
    } else {
        if !tls_config
            .cert_file()
            .exists()
        {
            return Err(VetisError::Config(ConfigError::tls("Certificate not found!")));
        }

        let cert = fs::read(tls_config.cert_file())
            .await
            .map_err(|e| {
                VetisError::Config(ConfigError::tls(format_compact!(
                    "Could not load certificate: {}",
                    e.to_string()
                )))
            })?;

        if !tls_config
            .key_file()
            .exists()
        {
            return Err(VetisError::Config(ConfigError::tls("Key not found!")));
        }

        let key = fs::read(tls_config.key_file())
            .await
            .map_err(|e| {
                VetisError::Config(ConfigError::tls(format_compact!(
                    "Could not load key: {}",
                    e.to_string()
                )))
            })?;

        Ok(Some(Tls::from_cert_and_key(&cert, &key)))
    }
}

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
        // Initialize TLS

        let tls = if let Some(tls_config) = host_config.tls() {
            build_tls(tls_config, host_config.root_directory()).await?
        } else {
            None
        };

        // Initialize hooks

        let mut env = Env::stdlib();
        env.add_library(extensions::request::Request::default())
            .map_err(|e| VetisError::matcher(e.to_compact_string()))?;

        let env = Arc::new(env);
        let mut hooks = Vec::new();

        if let Some(hooks_config) = host_config.hooks() {
            for hook_config in hooks_config {
                let mut hook = hook_config.boxed_hook();
                hook.setup().await?;
                if let Some(matcher) = hook.matcher_mut() {
                    matcher
                        .compile(env.clone())
                        .expect("Could not compile matcher!");
                }
                hooks.push(hook);
            }
        }

        hooks.sort_by_key(|hook| hook.phase());

        Ok(Self { config: host_config, tls, hooks, env })
    }

    /// Add a hook to the host
    ///
    /// # Arguments
    ///
    /// * `hook` - A `Hook` instance containing the path configuration.
    pub fn add_hook<H>(&mut self, mut hook: H)
    where
        H: VetisHook + 'static,
    {
        if let Some(matcher) = hook.matcher_mut() {
            matcher
                .compile(self.env.clone())
                .expect("Could not compile matcher!");
        }
        self.hooks
            .push(Box::new(hook));

        self.hooks
            .sort_by_key(|hook| hook.phase());
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
    pub fn tls(&self) -> Option<&Tls> {
        self.tls.as_ref()
    }
}

impl vetis::host::Host for Host {
    fn config(&self) -> &HostConfig {
        &self.config
    }

    fn config_mut(&mut self) -> &mut HostConfig {
        &mut self.config
    }

    fn hooks(&self) -> &VetisHooks {
        &self.hooks
    }

    fn hooks_mut(&mut self) -> &mut VetisHooks {
        &mut self.hooks
    }

    fn serve_status_page<'a>(
        &'a self,
        error: RuntimeError,
        logger: Option<Logger<LogSender>>,
    ) -> VetisFutureResult<'a, Response> {
        let future = async move {
            let mut response_builder = Response::builder().status(
                error
                    .status_code()
                    .clone(),
            );

            if let Some(headers) = error.headers() {
                response_builder = response_builder.headers(headers.clone());
            }

            let static_status_response = response_builder.text(
                error
                    .status_code()
                    .canonical_reason()
                    .unwrap_or("Unknown status code"),
            );

            if let Some(status_pages) = &self
                .config
                .status_pages()
            {
                if let Some(page) = status_pages.get(
                    &error
                        .status_code()
                        .as_u16(),
                ) {
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
                                    .status(
                                        error
                                            .status_code()
                                            .into(),
                                    )
                                    .body(HttpBody::stream(body)));
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

    fn route<'a>(
        &'a self,
        mut request: Request,
        logger: Option<Logger<LogSender>>,
    ) -> VetisFutureResult<'a, Response> {
        let uri_path: String = request
            .uri()
            .path()
            .into();

        let mut response = None;
        let future = async move {
            if uri_path.contains("/./") || uri_path.contains("/../") || uri_path.contains("..") {
                debug!(logger, "Invalid access!");
                return self
                    .serve_status_page(
                        RuntimeError::new("Invalid access!")
                            .with_status_code(StatusCode::FORBIDDEN),
                        logger,
                    )
                    .await;
            }

            let mut expr_context = Context::with_env(self.env.clone());
            let mut request_ref = request.as_ref();
            #[allow(unused)]
            let mut response_ref = None;
            expr_context
                .add_variable("request", request_ref)
                .map_err(|e| {
                    VetisError::Host(HostError::Runtime(RuntimeError::new(&e.to_string())))
                })?;

            for hook in &self.hooks {
                if hook.has_matches(&expr_context) {
                    let result = match hook.phase() {
                        HookPhase::PreProcess | HookPhase::Rewrite | HookPhase::Access
                            if response.is_none() =>
                        {
                            hook.handle(request, response)
                                .await
                        }
                        HookPhase::Content if response.is_none() => {
                            // TODO: Add cache lookup and short circuit
                            hook.handle(request, response)
                                .await
                            // TODO: Put response on cache
                        }
                        HookPhase::PostContent => {
                            hook.handle(request, response)
                                .await
                        }
                        _ => {
                            continue;
                        }
                    };

                    if let Err(VetisError::Host(HostError::Runtime(e))) = result {
                        error!(logger, target: "vetis", "Error: {}", e.to_string());
                        error!(logger, target: self.config.hostname(), "Error: {}", e.to_string());
                        return self
                            .serve_status_page(e, logger)
                            .await;
                    }

                    (request, response) = result.unwrap();

                    request_ref = request.as_ref();
                    expr_context
                        .add_variable("request", request_ref)
                        .map_err(|e| {
                            VetisError::Host(HostError::Runtime(RuntimeError::new(&e.to_string())))
                        })?;

                    if let Some(ref response) = response {
                        response_ref = Some(response.as_ref());
                        expr_context
                            .add_variable("response", response_ref)
                            .map_err(|e| {
                                VetisError::Host(HostError::Runtime(RuntimeError::new(
                                    &e.to_string(),
                                )))
                            })?;
                    }
                }
            }

            let Some(response) = response.take() else {
                return self
                    .serve_status_page(
                        RuntimeError::new("Not found").with_status_code(StatusCode::NOT_FOUND),
                        logger,
                    )
                    .await;
            };

            return Ok(response);
        };

        Box::pin(future)
    }
}
