use crate::{
    host::{
        Host,
        path::{HandlerPath, handler_fn},
    },
    tests::{
        CA_CERT, IP6_SERVER_CERT, IP6_SERVER_KEY, SERVER_CERT, SERVER_KEY, default_protocol_version,
    },
};
use deboa::cert::{CertificateExt, ContentEncoding};
use deboa_tokio::{Client, cert::DeboaCertificate};
use http::StatusCode;
use vetis::{VetisServer, VetisTestResult, host::HostConfig, security::TlsConfig};

#[tokio::test]
async fn test_no_https_error() -> VetisTestResult<()> {
    let localhost_config = HostConfig::builder()
        .hostname("localhost")
        .bind_addresses(&[(
            "0.0.0.0"
                .parse()
                .unwrap(),
            55000,
        )])
        .build()?;

    let mut localhost_host = Host::new(localhost_config).await?;

    let ip4_root_path = HandlerPath::builder()
        .uri("/hello")
        .handler(handler_fn(|_request, _ctx| async move {
            let response = vetis::Response::builder()
                .status(StatusCode::OK)
                .text("Hello from ipv4");
            Ok(response)
        }))
        .build()?;

    localhost_host.add_path(ip4_root_path);

    let mut server = crate::Vetis::builder()
        .add_host(localhost_host)
        .await?
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
    let ip4_root_path = HandlerPath::builder()
        .uri("/hello")
        .handler(handler_fn(|_request, _ctx| async move {
            let response = vetis::Response::builder()
                .status(StatusCode::OK)
                .text("Hello from ipv4");
            Ok(response)
        }))
        .build()?;

    // TODO: Add a path to config, even HandlerPath, build host from config
    let localhost_config = HostConfig::builder()
        .hostname("localhost")
        .bind_addresses(&[(
            "0.0.0.0"
                .parse()
                .unwrap(),
            55002,
        )])
        .build()?;

    let mut localhost_host = Host::new(localhost_config).await?;
    localhost_host.add_path(ip4_root_path);

    let mut server = crate::Vetis::builder()
        .add_host(localhost_host)
        .await?
        .build();

    server
        .start()
        .await?;

    let client = Client::builder()
        .protos(vec![default_protocol_version()])
        .prior_knowledge(true)
        .build();

    let response = deboa::request::get("http://localhost:55002/hello")?
        .version(default_protocol_version())
        .send_with(&client)
        .await?;

    assert_eq!(response.status(), StatusCode::OK);

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

    #[cfg(unix)]
    let ip6_security_config = TlsConfig::builder()
        .ca_file(CA_CERT)
        .cert_file(IP6_SERVER_CERT)
        .key_file(IP6_SERVER_KEY)
        .build()?;

    #[cfg(windows)]
    let ip6_security_config = SecurityConfig::builder()
        .ca_file(CA_CERT)
        .cert_file(SERVER_CERT)
        .key_file(SERVER_KEY)
        .build()?;

    let ip4_localhost_config = HostConfig::builder()
        .hostname("localhost")
        .tls(ip4_security_config)
        .bind_addresses(&[(
            "0.0.0.0"
                .parse()
                .unwrap(),
            65000,
        )])
        .build()?;

    let ip6_localhost_config = HostConfig::builder()
        .hostname(host)
        .tls(ip6_security_config)
        .bind_addresses(&[(
            "::".parse()
                .unwrap(),
            65001,
        )])
        .build()?;

    let mut ip4_localhost_host = Host::new(ip4_localhost_config).await?;
    let mut ip6_localhost_host = Host::new(ip6_localhost_config).await?;

    let ip4_root_path = HandlerPath::builder()
        .uri("/hello")
        .handler(handler_fn(|_request, _ctx| async move {
            let response = vetis::Response::builder()
                .status(StatusCode::OK)
                .text("Hello from ipv4");
            Ok(response)
        }))
        .build()?;

    let ip6_root_path = HandlerPath::builder()
        .uri("/hello")
        .handler(handler_fn(|_request, _ctx| async move {
            let response = vetis::Response::builder()
                .status(StatusCode::OK)
                .text("Hello from ipv6");
            Ok(response)
        }))
        .build()?;

    ip4_localhost_host.add_path(ip4_root_path);
    ip6_localhost_host.add_path(ip6_root_path);

    let mut server = crate::Vetis::builder()
        .add_host(ip4_localhost_host)
        .await?
        .add_host(ip6_localhost_host)
        .await?
        .build();

    server
        .start()
        .await?;

    let cert = tokio::fs::read(CA_CERT.to_string()).await?;
    let client = Client::builder()
        .certificate(DeboaCertificate::from_slice(&cert, ContentEncoding::DER))
        .build();

    let request = deboa::request::get("https://localhost:65000/hello")?
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
        .bind_addr(
            "::1"
                .parse()
                .unwrap(),
        )
        .build();

    let request = deboa::request::get(format!("https://{}:65001/hello", host))?
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
