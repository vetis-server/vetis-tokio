use crate::{
    hook::{HandlerHookConfig, handler_fn},
    tests::{
        CA_CERT, IP6_SERVER_CERT, IP6_SERVER_KEY, SERVER_CERT, SERVER_KEY, create_hello_hook,
        default_protocol_version,
    },
};
use deboa::{
    cert::{CertificateExt, ContentEncoding},
    request,
};
use deboa_tokio::{Client, cert::DeboaCertificate};
use http::{StatusCode, Version};
use rand::random_range;
use std::net::{IpAddr, Ipv4Addr};
use vetis::{VetisServer, VetisTestResult, host::HostConfig, security::TlsConfig};
use vetis_matchers::request::PathMatcherConfig;

#[tokio::test]
async fn test_no_https_error() -> VetisTestResult<()> {
    let ip4_root_hook = HandlerHookConfig::builder()
        .matcher(
            PathMatcherConfig::builder()
                .add_path("/hello*")
                .build()?,
        )
        .handler(handler_fn(|_request| async move {
            let response = vetis::Response::builder()
                .status(StatusCode::OK)
                .text("Hello from ipv4");
            Ok(response)
        }))
        .build()?;

    let localhost_config = HostConfig::builder()
        .hostname("localhost")
        .add_proto(Version::HTTP_11)
        .add_bind_address(Ipv4Addr::UNSPECIFIED, 55000)
        .add_hook(ip4_root_hook)
        .build()?;

    let mut server = crate::Vetis::builder()
        .add_host(localhost_config)
        .build();

    server
        .start()
        .await?;

    let client = Client::default();

    let response = deboa::request::get("https://localhost:55000/hello")?
        .send_with(&client)
        .await;

    assert!(response.is_err());

    Ok(())
}

#[tokio::test]
async fn test_http() -> VetisTestResult<()> {
    let ip4_root_path = HandlerHookConfig::builder()
        .matcher(
            PathMatcherConfig::builder()
                .add_path("/hello*")
                .build()?,
        )
        .handler(handler_fn(|_request| async move {
            let response = vetis::Response::builder()
                .status(StatusCode::OK)
                .text("Hello from ipv4");
            Ok(response)
        }))
        .build()?;

    // TODO: Add a path to config, even HandlerHook, build host from config
    let localhost_config = HostConfig::builder()
        .hostname("localhost")
        .add_default_header("x-vetis-test", "default")
        .add_proto(Version::HTTP_11)
        .add_bind_address(Ipv4Addr::UNSPECIFIED, 55002)
        .add_hook(ip4_root_path)
        .build()?;

    let mut server = crate::Vetis::builder()
        .add_host(localhost_config)
        .build();

    server
        .start()
        .await?;

    let client = Client::default();

    let response = deboa::request::get("http://localhost:55002/hello")?
        .send_with(&client)
        .await?;

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["x-vetis-test"], "default");

    Ok(())
}

#[tokio::test]
async fn test_http_redirects_to_tls_when_hsts_is_enabled() -> VetisTestResult<()> {
    let port = random_range(9000..=20000);
    let tls = TlsConfig::builder()
        .ca_file(CA_CERT)
        .cert_file(SERVER_CERT)
        .key_file(SERVER_KEY)
        .build()?;
    let config = HostConfig::builder()
        .hostname("localhost")
        .tls(tls)
        .allow_unsafe_connections(true)
        .enable_hsts(true)
        .add_proto(Version::HTTP_11)
        .add_bind_address(Ipv4Addr::UNSPECIFIED, port)
        .add_hook(create_hello_hook())
        .build()?;
    let mut server = crate::Vetis::builder()
        .add_host(config)
        .build();

    server
        .start()
        .await?;

    let response = request::get(format!("http://localhost:{port}/hello"))?
        .send_with(&Client::default())
        .await?;

    assert_eq!(response.status(), StatusCode::PERMANENT_REDIRECT);
    assert_eq!(response.headers()["location"], format!("https://localhost:{port}/hello"));
    server
        .stop()
        .await?;
    Ok(())
}

#[tokio::test]
async fn test_unknown_host() -> VetisTestResult<()> {
    let port = random_range(9000..=20000);
    let host = HostConfig::builder()
        .hostname("localhost")
        .allow_unsafe_connections(true)
        .add_proto(Version::HTTP_11)
        .add_bind_address(Ipv4Addr::LOCALHOST, port)
        .add_hook(create_hello_hook())
        .build()?;
    let mut server = crate::Vetis::builder()
        .add_host(host)
        .build();

    server
        .start()
        .await?;

    let response = request::get(format!("http://127.0.0.1:{port}/hello"))?
        .send_with(&Client::default())
        .await?;

    assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
    server
        .stop()
        .await?;
    Ok(())
}

#[tokio::test]
async fn test_multiple_interfaces() -> VetisTestResult<()> {
    let host = if cfg!(windows) { "localhost" } else { "ip6-localhost" };

    let ip4_security_config = TlsConfig::builder()
        .ca_file(CA_CERT)
        .cert_file(SERVER_CERT)
        .key_file(SERVER_KEY)
        .build()?;

    let ip4_root_hook = HandlerHookConfig::builder()
        .matcher(
            PathMatcherConfig::builder()
                .add_path("/hello*")
                .build()?,
        )
        .handler(handler_fn(|_request| async move {
            let response = vetis::Response::builder()
                .status(StatusCode::OK)
                .text("Hello from ipv4");
            Ok(response)
        }))
        .build()?;

    let ip4_localhost_config = HostConfig::builder()
        .hostname("localhost")
        .tls(ip4_security_config)
        .add_proto(default_protocol_version())
        .add_bind_address(
            "0.0.0.0"
                .parse::<IpAddr>()
                .unwrap(),
            65000,
        )
        .add_hook(ip4_root_hook)
        .build()?;

    #[cfg(unix)]
    let ip6_security_config = TlsConfig::builder()
        .ca_file(CA_CERT)
        .cert_file(IP6_SERVER_CERT)
        .key_file(IP6_SERVER_KEY)
        .build()?;

    #[cfg(windows)]
    let ip6_security_config = TlsConfig::builder()
        .ca_file(CA_CERT)
        .cert_file(SERVER_CERT)
        .key_file(SERVER_KEY)
        .build()?;

    let ip6_root_hook = HandlerHookConfig::builder()
        .matcher(
            PathMatcherConfig::builder()
                .add_path("/hello*")
                .build()?,
        )
        .handler(handler_fn(|_request| async move {
            let response = vetis::Response::builder()
                .status(StatusCode::OK)
                .text("Hello from ipv6");
            Ok(response)
        }))
        .build()?;

    let ip6_localhost_config = HostConfig::builder()
        .hostname(host)
        .tls(ip6_security_config)
        .add_proto(default_protocol_version())
        .add_bind_address(
            "::".parse::<IpAddr>()
                .unwrap(),
            65001,
        )
        .add_hook(ip6_root_hook)
        .build()?;

    let mut server = crate::Vetis::builder()
        .add_host(ip4_localhost_config)
        .add_host(ip6_localhost_config)
        .build();

    server
        .start()
        .await?;

    let cert = tokio::fs::read(CA_CERT.to_string()).await?;
    let client = Client::builder()
        .certificate(DeboaCertificate::from_slice(&cert, ContentEncoding::DER))
        .protos(vec![default_protocol_version()])
        .prior_knowledge(true)
        .build();

    let request = deboa::request::get("https://localhost:65000/hello")?
        .version(default_protocol_version())
        .send_with(&client)
        .await?;

    assert_eq!(request.status(), StatusCode::OK);
    assert_eq!(
        request
            .text()
            .await?,
        "Hello from ipv4"
    );

    let cert = tokio::fs::read(CA_CERT.to_string()).await?;
    let client = Client::builder()
        .certificate(DeboaCertificate::from_slice(&cert, ContentEncoding::DER))
        .protos(vec![default_protocol_version()])
        .prior_knowledge(true)
        .bind_addr(
            "::1"
                .parse()
                .unwrap(),
        )
        .build();

    let request = deboa::request::get(format!("https://{}:65001/hello", host))?
        .version(default_protocol_version())
        .send_with(&client)
        .await?;

    assert_eq!(request.status(), StatusCode::OK);
    assert_eq!(
        request
            .text()
            .await?,
        "Hello from ipv6"
    );

    server
        .stop()
        .await?;

    Ok(())
}
