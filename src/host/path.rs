//! Path module
use vetis::{
    Request, Response, Str, VetisFutureResult, VetisResult,
    errors::{HandlerError, HostError, VetisError},
    host::{HostContext, path::Path},
};

/// Type alias for boxed handler closures.
///
/// This represents an async function that takes a `Request` and returns
/// a `Response` or an error. Handlers are the core of request processing
/// in VeTiS hosts.
///
/// # Examples
///
/// ```rust,no_run
/// use vetis::HandlerFn;
/// use vetis::{Request, Response, errors::VetisError};
///
/// let handler: HandlerFn = Box::new(|request: Request| {
///     Box::pin(async move {
///         // Process request...
///         Ok(Response::builder()
///             .status(http::StatusCode::OK)
///             .text("OK"))
///     })
/// });
/// ```
pub type HandlerFn =
    Box<dyn Fn(Request, HostContext) -> VetisFutureResult<'static, Response> + Send + Sync>;

/// Creates a handler function from a function.
///
/// This utility function converts any compatible async function into a
/// `HandlerFn` that can be used with hosts.
///
/// # Arguments
///
/// * `f` - An async function that takes a `Request` and returns a `VetisResult<Response>`
///
/// # Examples
///
/// ```rust,no_run
/// use vetis::{
///     host::{handler_fn, HostConfig},
/// };
///
/// let config = HostConfig::builder()
///     .hostname("example.com")
///     .build()
///     .unwrap();
///
/// assert_eq!("example.com", config.hostname());
/// ```
pub fn handler_fn<F, Fut>(f: F) -> HandlerFn
where
    F: Fn(Request, HostContext) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = VetisResult<Response>> + Send + Sync + 'static,
{
    Box::new(move |req, ctx| Box::pin(f(req, ctx)))
}

/// Builder for handler path
pub struct HandlerPathBuilder {
    uri: Str,
    handler: Option<HandlerFn>,
}

impl HandlerPathBuilder {
    /// Allow set handler uri path
    ///
    /// # Arguments
    ///
    /// * `uri` - The uri of the handler path
    ///
    /// # Returns
    ///
    /// * `Self` - The builder
    pub fn uri(mut self, uri: &str) -> Self {
        self.uri = Str::from(uri.to_string());
        self
    }

    /// Allow set handler function
    ///
    /// # Arguments
    ///
    /// * `handler` - The handler function
    ///
    /// # Returns
    ///
    /// * `Self` - The builder
    pub fn handler(mut self, handler: HandlerFn) -> Self {
        self.handler = Some(handler);
        self
    }

    /// Build the handler path
    ///
    /// # Returns
    ///
    /// * `Result<HandlerPath, VetisError>` - The handler path or error
    pub fn build(self) -> Result<HandlerPath, VetisError> {
        if self.uri.is_empty() {
            return Err(VetisError::Host(HostError::Handler(HandlerError::Uri(
                "URI cannot be empty".to_string(),
            ))));
        }

        let handler = match self.handler {
            Some(handler) => handler,
            None => {
                return Err(VetisError::Host(HostError::Handler(HandlerError::Handler(
                    "Handler must be set".to_string(),
                ))));
            }
        };

        Ok(HandlerPath { uri: self.uri.into(), handler })
    }
}

/// Handler path
pub struct HandlerPath {
    uri: Str,
    handler: HandlerFn,
}

impl HandlerPath {
    /// Allow create a new handler path builder
    ///
    /// # Returns
    ///
    /// * `HandlerPathBuilder` - The builder
    pub fn builder() -> HandlerPathBuilder {
        HandlerPathBuilder { uri: "/".into(), handler: None }
    }
}

impl Path for HandlerPath {
    /// Allow get handler uri path
    ///
    /// # Returns
    ///
    /// * `&str` - The uri of the handler path
    fn uri(&self) -> &str {
        self.uri.as_ref()
    }

    /// Handles the request for the path
    ///
    /// # Arguments
    ///
    /// * `request` - The request to handle
    /// * `host_context` - The host context for this path
    ///'
    /// # Returns
    ///
    /// * `VetisFutureResult<'a, Response>` - The future that will handle the request
    fn handle<'a>(
        &'a self,
        request: Request,
        host_context: HostContext,
    ) -> VetisFutureResult<'a, Response> {
        (self.handler)(request, host_context)
    }
}
