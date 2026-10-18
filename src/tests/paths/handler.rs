use crate::{
    host::{
        Host,
        path::{HandlerFn, HandlerPath, handler_fn},
    },
    tests::{CA_CERT, SERVER_CERT, SERVER_KEY, default_protocol_version},
};
use caramelo::{
    expect,
    matchers::{eq, truthy},
};
use deboa::{
    cert::{CertificateExt, ContentEncoding},
    request,
};
use deboa_tokio::{Client, HttpBody, cert::DeboaCertificate};
use http::{Request, StatusCode};
use rand::random_range;
use vetis::{
    VetisServer as _,
    errors::{ContentError, HostError, VetisError},
    host::{Host as _, HostConfig},
    security::TlsConfig,
};
use vetis_macros::status_pages;

fn create_hello_handler() -> HandlerFn {
    handler_fn(|_request, _ctx| async move {
        let response = vetis::Response::builder()
            .status(StatusCode::OK)
            .text("Hello from localhost");
        Ok(response)
    })
}

async fn create_host(
    handler: HandlerFn,
    port: u16,
    ca: &str,
    cert: &str,
    key: &str,
) -> Result<Host, Box<dyn std::error::Error>> {
    let security_config = TlsConfig::builder()
        .ca_file(ca)
        .cert_file(cert)
        .key_file(key)
        .build()?;

    let host_config = HostConfig::builder()
        .hostname("localhost")
        .root_directory(".")
        .protos(&[default_protocol_version()])
        .tls(security_config)
        .status_pages(status_pages! {
            404 @ "files/404.html"
        })
        .bind_addresses(&[(
            "0.0.0.0"
                .parse()
                .unwrap(),
            port,
        )])
        .build()?;

    let root_path = HandlerPath::builder()
        .uri("/hello")
        .handler(handler)
        .build()?;

    let mut host = Host::new(host_config).await?;

    host.add_path(root_path);

    Ok(host)
}

#[tokio::test]
async fn test_handler() -> Result<(), Box<dyn std::error::Error>> {
    let port = random_range(9000..=20000);

    let mut server = crate::Vetis::builder()
        .add_host(
            create_host(create_hello_handler(), port, CA_CERT, SERVER_CERT, SERVER_KEY).await?,
        )
        .await?
        .build();

    server
        .start()
        .await?;

    let cert = tokio::fs::read(CA_CERT).await?;
    let client = Client::builder()
        .protos(vec![default_protocol_version()])
        .certificate(DeboaCertificate::from_slice(&cert, ContentEncoding::DER))
        .prior_knowledge(true)
        .build();

    let request = request::get(format!("https://localhost:{}{}", port, "/hello"))?
        .version(default_protocol_version())
        .send_with(&client)
        .await?;

    assert_eq!(request.status(), StatusCode::OK);

    server
        .stop()
        .await?;

    Ok(())
}

#[tokio::test]
async fn test_missing_path() -> Result<(), Box<dyn std::error::Error>> {
    let port = random_range(9000..=20000);
    let host = create_host(create_hello_handler(), port, CA_CERT, SERVER_CERT, SERVER_KEY).await?;
    let original_request =
        Request::get(format!("https://localhost:{}/not-found", port)).body(HttpBody::empty())?;
    let request = vetis::Request::new(original_request);
    let response = host
        .route(request, None)
        .await?;
    let inner_response = response.into_inner();
    expect(inner_response.status()).to_be(eq(StatusCode::NOT_FOUND));
    Ok(())
}

#[tokio::test]
async fn test_forbidden_traversal() -> Result<(), Box<dyn std::error::Error>> {
    let port = random_range(9000..=20000);
    let host = create_host(create_hello_handler(), port, CA_CERT, SERVER_CERT, SERVER_KEY).await?;
    let original_request =
        Request::get(format!("https://localhost:{}/../hello", port)).body(HttpBody::empty())?;
    let request = vetis::Request::new(original_request);
    let response = host
        .route(request, None)
        .await?;
    let inner_response = response.into_inner();
    expect(inner_response.status()).to_be(eq(StatusCode::FORBIDDEN));
    Ok(())
}

#[tokio::test]
async fn test_cert_not_exits() -> Result<(), Box<dyn std::error::Error>> {
    let port = random_range(9000..=20000);
    let host = create_host(create_hello_handler(), port, CA_CERT, "/some/cert", SERVER_KEY).await;
    expect(host.is_err()).to_be(truthy());
    Ok(())
}

#[tokio::test]
async fn test_key_not_exits() -> Result<(), Box<dyn std::error::Error>> {
    let port = random_range(9000..=20000);
    let host = create_host(create_hello_handler(), port, CA_CERT, SERVER_CERT, "/some/key").await;
    expect(host.is_err()).to_be(truthy());
    Ok(())
}

#[tokio::test]
async fn test_auth_error() -> Result<(), Box<dyn std::error::Error>> {
    let handler = handler_fn(|_request, _ctx| async move {
        Err(VetisError::Host(HostError::Auth("Nothing".into())))
    });

    let port = random_range(9000..=20000);
    let host = create_host(handler, port, CA_CERT, SERVER_CERT, SERVER_KEY).await?;
    let original_request =
        Request::get(format!("https://localhost:{}/hello", port)).body(HttpBody::empty())?;
    let request = vetis::Request::new(original_request);
    let response = host
        .route(request, None)
        .await?;
    let inner_response = response.into_inner();
    expect(inner_response.status()).to_be(eq(StatusCode::UNAUTHORIZED));
    Ok(())
}

#[tokio::test]
async fn test_proxy_error() -> Result<(), Box<dyn std::error::Error>> {
    let handler = handler_fn(|_request, _ctx| async move {
        Err(VetisError::Host(HostError::Proxy("Nothing".into())))
    });

    let port = random_range(9000..=20000);
    let host = create_host(handler, port, CA_CERT, SERVER_CERT, SERVER_KEY).await?;
    let original_request =
        Request::get(format!("https://localhost:{}/hello", port)).body(HttpBody::empty())?;
    let request = vetis::Request::new(original_request);
    let response = host
        .route(request, None)
        .await?;
    let inner_response = response.into_inner();
    expect(inner_response.status()).to_be(eq(StatusCode::BAD_GATEWAY));
    Ok(())
}

#[tokio::test]
async fn test_content_error() -> Result<(), Box<dyn std::error::Error>> {
    let handler = handler_fn(|_request, _ctx| async move {
        Err(VetisError::Host(HostError::Content(ContentError::Forbidden)))
    });

    let port = random_range(9000..=20000);
    let host = create_host(handler, port, CA_CERT, SERVER_CERT, SERVER_KEY).await?;
    let original_request =
        Request::get(format!("https://localhost:{}/hello", port)).body(HttpBody::empty())?;
    let request = vetis::Request::new(original_request);
    let response = host
        .route(request, None)
        .await?;
    let inner_response = response.into_inner();
    expect(inner_response.status()).to_be(eq(StatusCode::FORBIDDEN));
    Ok(())
}
