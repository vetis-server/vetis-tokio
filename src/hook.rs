//! Hook module
use cel::Context;
use hyper_body_utils::HttpBody;
use vetis::{
    Request, Response, VetisFutureResult, VetisResult,
    errors::{ConfigError, VetisError},
    hook::{Hook, HookPhase},
    matcher::Matcher,
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
/// use vetis::{Request, Response, errors::VetisError};
/// use vetis_tokio::hook::HandlerFn;
///
/// let handler: HandlerFn = Box::new(|_request: Request| {
///     Box::pin(async move {
///         // Process request...
///         let response =
///             Response::builder()
///                 .status(http::StatusCode::OK)
///                 .text("OK");
///         Ok(response)
///     })
/// });
/// ```
pub type HandlerFn = Box<dyn Fn(Request) -> VetisFutureResult<'static, Response> + Send + Sync>;

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
///     host::{HostConfig},
/// };
/// use vetis_tokio::hook::handler_fn;
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
    F: Fn(Request) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = VetisResult<Response>> + Send + Sync + 'static,
{
    Box::new(move |req| Box::pin(f(req)))
}

/// Builder for handler hook
pub struct HandlerHookBuilder {
    matcher: Option<Box<dyn Matcher>>,
    handler: Option<HandlerFn>,
}

impl HandlerHookBuilder {
    /// Allow set a matcher
    ///
    /// # Returns
    ///
    /// * `Self` - The builder.
    pub fn matcher<M>(mut self, matcher: M) -> Self
    where
        M: Matcher + Send + Sync + 'static,
    {
        self.matcher = Some(Box::new(matcher));
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

    /// Build the handler hook
    ///
    /// # Returns
    ///
    /// * `Result<HandlerHook, VetisError>` - The handler path or error
    pub fn build(self) -> Result<HandlerHook, VetisError> {
        let handler = match self.handler {
            Some(handler) => handler,
            None => {
                return Err(VetisError::Config(ConfigError::hook("Handler must be set")));
            }
        };

        Ok(HandlerHook {
            matcher: self
                .matcher
                .map_or(None, |val| Some(val)),
            handler,
        })
    }
}

/// Handler path
pub struct HandlerHook {
    matcher: Option<Box<dyn Matcher>>,
    handler: HandlerFn,
}

unsafe impl Send for HandlerHook {}
unsafe impl Sync for HandlerHook {}

impl HandlerHook {
    /// Allow create a new handler hook builder
    ///
    /// # Returns
    ///
    /// * `HandlerHookBuilder` - The builder
    pub fn builder() -> HandlerHookBuilder {
        HandlerHookBuilder { matcher: None, handler: None }
    }
}

impl Hook for HandlerHook {
    fn has_matches(&self, context: &Context) -> bool {
        if let Some(matcher) = &self.matcher { matcher.is_match(context) } else { true }
    }

    fn phase(&self) -> &HookPhase {
        &HookPhase::Content
    }

    fn matcher_mut(&mut self) -> Option<&mut Box<dyn Matcher>> {
        self.matcher
            .as_mut()
    }

    fn handle<'a>(
        &'a self,
        request: Request,
        _response: Option<Response>,
    ) -> VetisFutureResult<'a, (Request, Option<Response>)> {
        let future = async move {
            let (parts, body) = request.into_parts();
            let handler_request = Request::from_parts(parts.clone(), body);
            let response = (self.handler)(handler_request).await?;
            let request = Request::from_parts(parts, HttpBody::empty());
            Ok((request, Some(response)))
        };
        Box::pin(future)
    }
}
