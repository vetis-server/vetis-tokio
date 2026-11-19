use crate::{
    hook::{HandlerFn, HandlerHook},
    host::Host,
    service::quic::HttpService,
};
use http::StatusCode;
use hyper::service::Service;
use hyper_body_utils::HttpBody;
use std::{net::SocketAddr, sync::Arc};
use vetis::{VetisHosts, errors::RuntimeError, host::HostConfig};
use vetis_matchers::request::{PathMatcher, PathMatcherConfig};

async fn create_service(
    handler: HandlerFn,
    default_headers: &[(&str, &str)],
) -> Result<HttpService<Host>, Box<dyn std::error::Error>> {
    let mut config = HostConfig::builder().hostname("localhost");
    for (name, value) in default_headers {
        config = config.header(name, value);
    }
    let mut host = Host::new(config.build()?).await?;
    host.add_hook(
        HandlerHook::builder()
            .matcher(PathMatcher::new(
                PathMatcherConfig::builder()
                    .add_path("/ok*")
                    .build()?,
            ))
            .handler(handler)
            .build()?,
    );

    let hosts = papaya::HashMap::new();
    hosts
        .pin_owned()
        .insert("localhost".into(), Arc::new(host));
    Ok(HttpService::new(
        VetisHosts::new(hosts.into()),
        SocketAddr::from(([127, 0, 0, 1], 12345)),
        None,
    ))
}

#[tokio::test]
async fn http_service_returns_bad_request_without_authority() {
    let service = create_service(crate::tests::create_hello_handler(), &[])
        .await
        .unwrap();
    let request = http::Request::builder()
        .uri("/ok")
        .body(HttpBody::empty())
        .unwrap();

    let response = <HttpService<Host> as Service<http::Request<HttpBody>>>::call(&service, request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn http_service_returns_bad_request_for_unknown_host() {
    let service = create_service(crate::tests::create_hello_handler(), &[])
        .await
        .unwrap();
    let request = http::Request::builder()
        .uri("http://unknown.example/ok")
        .body(HttpBody::empty())
        .unwrap();

    let response = <HttpService<Host> as Service<http::Request<HttpBody>>>::call(&service, request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn http_service_maps_handler_errors_to_internal_server_error() {
    let handler = crate::tests::handler_fn(|_request| async move {
        Err(RuntimeError::new("handler failed").into())
    });
    let service = create_service(handler, &[])
        .await
        .unwrap();
    let request = http::Request::builder()
        .uri("http://localhost/ok")
        .body(HttpBody::empty())
        .unwrap();

    let response = <HttpService<Host> as Service<http::Request<HttpBody>>>::call(&service, request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
}

#[tokio::test]
async fn http_service_skips_invalid_default_headers() {
    let service = create_service(
        crate::tests::create_hello_handler(),
        &[("x-good", "accepted"), ("bad header", "invalid name"), ("x-bad", "invalid\nvalue")],
    )
    .await
    .unwrap();
    let request = http::Request::builder()
        .uri("http://localhost/ok")
        .body(HttpBody::empty())
        .unwrap();

    let response = <HttpService<Host> as Service<http::Request<HttpBody>>>::call(&service, request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["x-good"], "accepted");
    assert!(
        !response
            .headers()
            .contains_key("bad header")
    );
    assert!(
        !response
            .headers()
            .contains_key("x-bad")
    );
}
