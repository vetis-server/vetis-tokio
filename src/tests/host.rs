use std::net::{IpAddr, Ipv4Addr};

use crate::{
    hook::{HandlerHookConfig, handler_fn},
    host::Host,
    tests::{CA_CERT, SERVER_CERT, SERVER_KEY, create_hello_handler, create_host},
};
use caramelo::{expect, matchers::truthy};
use http::StatusCode;
use rand::random_range;
use vetis::host::{Host as _, HostConfig};

#[tokio::test]
async fn test_add_host() -> Result<(), Box<dyn std::error::Error>> {
    let config = HostConfig::builder()
        .hostname("localhost")
        .root_directory("src/tests")
        .add_bind_address(Ipv4Addr::UNSPECIFIED, 8080)
        .add_hook(
            HandlerHookConfig::builder()
                .handler(handler_fn(|_request| async move {
                    let response = vetis::Response::builder()
                        .status(StatusCode::OK)
                        .text("Hello, world!");
                    Ok(response)
                }))
                .build()
                .unwrap(),
        )
        .build()
        .unwrap();

    assert_eq!(config.hostname(), "localhost");

    Ok(())
}

#[tokio::test]
async fn test_host_tls_and_mutable_config() -> Result<(), Box<dyn std::error::Error>> {
    let config = HostConfig::builder()
        .hostname("before")
        .add_bind_address(Ipv4Addr::UNSPECIFIED, 8080)
        .build()?;
    let mut host = Host::new(config).await?;
    let cert = tokio::fs::read(SERVER_CERT).await?;
    let key = tokio::fs::read(SERVER_KEY).await?;

    host.add_tls(vetis::security::Tls::from_cert_and_key(&cert, &key));
    assert!(host.tls().is_some());

    *host.config_mut() = HostConfig::builder()
        .hostname("after")
        .add_bind_address(Ipv4Addr::UNSPECIFIED, 8080)
        .build()?;
    assert_eq!(
        host.config()
            .hostname(),
        "after"
    );
    Ok(())
}

#[tokio::test]
async fn test_cert_not_exits() -> Result<(), Box<dyn std::error::Error>> {
    let port = random_range(9000..=20000);
    let host =
        create_host(create_hello_handler(), "localhost", port, CA_CERT, "/some/cert", SERVER_KEY)
            .await;
    expect(host.is_err()).to_be(truthy());
    Ok(())
}

#[tokio::test]
async fn test_key_not_exits() -> Result<(), Box<dyn std::error::Error>> {
    let port = random_range(9000..=20000);
    let host =
        create_host(create_hello_handler(), "localhost", port, CA_CERT, SERVER_CERT, "/some/key")
            .await;
    expect(host.is_err()).to_be(truthy());
    Ok(())
}
