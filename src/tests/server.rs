use crate::{
    host::{path::HandlerPath, Host},
    listener::build_listeners,
    tests::{
        default_protocol_version, CA_CERT, IP6_SERVER_CERT, IP6_SERVER_KEY, SERVER_CERT, SERVER_KEY,
    },
};
use deboa::cert::{CertificateExt, ContentEncoding};
use deboa_tokio::{cert::DeboaCertificate, Client};
use http::StatusCode;
use std::{error::Error, net::IpAddr};
use vetis::{
    errors::VetisError,
    host::{handler_fn, HostConfig},
    listener::ListenerConfig,
    security::SecurityConfig,
    VetisServer,
};

fn create_listener(
    port: u16,
    interface: IpAddr,
    allow_unsafe: bool,
) -> Result<ListenerConfig, VetisError> {
    ListenerConfig::builder()
        .port(port)
        .protos(vec![default_protocol_version()])
        .interface(interface)
        .allow_unsafe_connections(allow_unsafe)
        .build()
}

#[tokio::test]
async fn test_no_https_error() -> Result<(), Box<dyn Error>> {
    let ipv4 = create_listener(
        55000,
        "0.0.0.0"
            .parse()
            .unwrap(),
        false,
    )?;

    let localhost_config = HostConfig::builder()
        .hostname("localhost")
        .bind_addresses(vec![(
            "0.0.0.0"
                .parse()
                .unwrap(),
            55000,
        )])
        .build()?;

    let mut localhost_host = Host::new(localhost_config);

    let ip4_root_path = HandlerPath::builder()
        .uri("/hello")
        .handler(handler_fn(|_request| async move {
            let response = vetis::Response::builder()
                .status(StatusCode::OK)
                .text("Hello from ipv4");
            Ok(response)
        }))
        .build()?;

    localhost_host.add_path(ip4_root_path);

    let mut server = crate::Vetis::builder()
        .add_listeners(build_listeners(ipv4))?
        .add_host(localhost_host)?
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
async fn test_http() -> Result<(), Box<dyn Error>> {
    let ipv4 = create_listener(
        55002,
        "0.0.0.0"
            .parse()
            .unwrap(),
        true,
    )?;

    let ip4_root_path = HandlerPath::builder()
        .uri("/hello")
        .handler(handler_fn(|_request| async move {
            let response = vetis::Response::builder()
                .status(StatusCode::OK)
                .text("Hello from ipv4");
            Ok(response)
        }))
        .build()?;

    // TODO: Add a path to config, even HandlerPath, build host from config
    let localhost_config = HostConfig::builder()
        .hostname("localhost")
        .bind_addresses(vec![(
            "0.0.0.0"
                .parse()
                .unwrap(),
            55002,
        )])
        .build()?;

    let mut localhost_host = Host::new(localhost_config);
    localhost_host.add_path(ip4_root_path);

    let mut server = crate::Vetis::builder()
        .add_listeners(build_listeners(ipv4))?
        .add_host(localhost_host)?
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
async fn test_multiple_interfaces() -> Result<(), Box<dyn Error>> {
    let host = if cfg!(windows) { "localhost" } else { "ip6-localhost" };

    let ipv4 = create_listener(
        65000,
        "0.0.0.0"
            .parse()
            .unwrap(),
        false,
    )?;

    let ipv6 = create_listener(
        65001,
        "::".parse()
            .unwrap(),
        false,
    )?;

    let ip4_security_config = SecurityConfig::builder()
        .ca_cert_from_bytes(CA_CERT.to_vec())
        .cert_from_bytes(SERVER_CERT.to_vec())
        .key_from_bytes(SERVER_KEY.to_vec())
        .build()?;

    #[cfg(unix)]
    let ip6_security_config = SecurityConfig::builder()
        .ca_cert_from_bytes(CA_CERT.to_vec())
        .cert_from_bytes(IP6_SERVER_CERT.to_vec())
        .key_from_bytes(IP6_SERVER_KEY.to_vec())
        .build()?;

    #[cfg(windows)]
    let ip6_security_config = SecurityConfig::builder()
        .ca_cert_from_bytes(CA_CERT.to_vec())
        .cert_from_bytes(SERVER_CERT.to_vec())
        .key_from_bytes(SERVER_KEY.to_vec())
        .build()?;

    let ip4_localhost_config = HostConfig::builder()
        .hostname("localhost")
        .security(ip4_security_config)
        .bind_addresses(vec![(
            "0.0.0.0"
                .parse()
                .unwrap(),
            65000,
        )])
        .build()?;

    let ip6_localhost_config = HostConfig::builder()
        .hostname(host)
        .security(ip6_security_config)
        .bind_addresses(vec![(
            "::".parse()
                .unwrap(),
            65001,
        )])
        .build()?;

    let mut ip4_localhost_host = Host::new(ip4_localhost_config);
    let mut ip6_localhost_host = Host::new(ip6_localhost_config);

    let ip4_root_path = HandlerPath::builder()
        .uri("/hello")
        .handler(handler_fn(|_request| async move {
            let response = vetis::Response::builder()
                .status(StatusCode::OK)
                .text("Hello from ipv4");
            Ok(response)
        }))
        .build()?;

    let ip6_root_path = HandlerPath::builder()
        .uri("/hello")
        .handler(handler_fn(|_request| async move {
            let response = vetis::Response::builder()
                .status(StatusCode::OK)
                .text("Hello from ipv6");
            Ok(response)
        }))
        .build()?;

    ip4_localhost_host.add_path(ip4_root_path);
    ip6_localhost_host.add_path(ip6_root_path);

    let mut server = crate::Vetis::builder()
        .add_listeners(build_listeners(ipv4))?
        .add_listeners(build_listeners(ipv6))?
        .add_host(ip4_localhost_host)?
        .add_host(ip6_localhost_host)?
        .build();

    server
        .start()
        .await?;

    let client = Client::builder()
        .certificate(DeboaCertificate::from_slice(CA_CERT, ContentEncoding::DER))
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

    let client = Client::builder()
        .certificate(DeboaCertificate::from_slice(CA_CERT, ContentEncoding::DER))
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
