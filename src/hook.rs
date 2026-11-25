//! Hook module
use std::sync::Arc;

use cel::Context;
use hyper_body_utils::HttpBody;
use serde::{Deserialize, Serialize};
use vetis::{
    Request, Response, VetisFutureResult, VetisHook, VetisResult,
    errors::VetisError,
    hook::{Hook, HookPhase},
    matcher::{Matcher, MatcherConfig},
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
pub type HandlerFn = Arc<dyn Fn(Request) -> VetisFutureResult<'static, Response> + Send + Sync>;

/// Creates a handler function from a function.
///
/// This utility function converts any compatible async function into a
/// `HandlerFn` that can be used with hosts.
///
/// # Arguments
///
/// * `func` - An async function that takes a `Request` and returns a `VetisResult<Response>`
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
pub fn handler_fn<F, Fut>(func: F) -> HandlerFn
where
    F: Fn(Request) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = VetisResult<Response>> + Send + Sync + 'static,
{
    Arc::new(move |req| Box::pin(func(req)))
}

/// Builder for handler hook
pub struct HandlerHookConfigBuilder {
    matcher: Option<Box<dyn MatcherConfig>>,
    handler: Option<HandlerFn>,
}

impl HandlerHookConfigBuilder {
    /// Allow set a matcher
    ///
    /// # Returns
    ///
    /// * `Self` - The builder.
    pub fn matcher<M>(mut self, matcher: M) -> Self
    where
        M: MatcherConfig + Send + Sync + 'static,
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
    /// * `Result<HandlerHookConfig, VetisError>` - The handler path or error
    pub fn build(self) -> Result<HandlerHookConfig, VetisError> {
        Ok(HandlerHookConfig {
            matcher: self
                .matcher
                .map_or(None, |val| Some(val)),
            handler: self.handler,
        })
    }
}

#[derive(Serialize, Deserialize)]
/// Handler hook configuration
pub struct HandlerHookConfig {
    matcher: Option<Box<dyn MatcherConfig>>,
    #[serde(skip)]
    handler: Option<HandlerFn>,
}

impl Clone for HandlerHookConfig {
    fn clone(&self) -> Self {
        HandlerHookConfig { matcher: self.matcher.clone(), handler: self.handler.clone() }
    }
}

impl Default for HandlerHookConfig {
    fn default() -> Self {
        HandlerHookConfig { matcher: None, handler: None }
    }
}

#[typetag::serde(name = "handler")]
impl vetis::hook::HookConfig for HandlerHookConfig {
    fn matcher(&self) -> Option<&Box<dyn MatcherConfig>> {
        self.matcher
            .as_ref()
    }

    fn boxed_clone(&self) -> Box<dyn vetis::hook::HookConfig> {
        Box::new(self.clone())
    }

    fn boxed_hook<'a>(&'a self) -> Box<dyn VetisHook> {
        Box::new(HandlerHook::new(self.clone()))
    }
}

impl HandlerHookConfig {
    /// Creates a new `HandlerHookConfigBuilder` with default settings.
    ///
    /// # Returns
    ///
    /// * `HandlerHookConfigBuilder` - The builder.
    pub fn builder() -> HandlerHookConfigBuilder {
        HandlerHookConfigBuilder { matcher: None, handler: None }
    }
}

/// Handler path
pub struct HandlerHook {
    matcher: Option<Box<dyn Matcher>>,
    config: HandlerHookConfig,
}

unsafe impl Send for HandlerHook {}
unsafe impl Sync for HandlerHook {}

impl HandlerHook {
    /// Build a new BasicAuth out of its configuration
    pub fn new(config: HandlerHookConfig) -> Self {
        HandlerHook { matcher: None, config }
    }
}

impl Hook for HandlerHook {
    fn setup<'a>(&'a mut self) -> VetisFutureResult<'a, ()> {
        Box::pin(async move { Ok(()) })
    }

    fn has_matches(&self, context: &Context) -> bool {
        if let Some(matcher) = &self.matcher { matcher.is_match(context) } else { true }
    }

    fn phase(&self) -> HookPhase {
        HookPhase::Content
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
            let response = if let Some(handler) = &self.config.handler {
                Some((handler)(handler_request).await?)
            } else {
                None
            };
            let request = Request::from_parts(parts, HttpBody::empty());
            Ok((request, response))
        };
        Box::pin(future)
    }
}
