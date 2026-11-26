use std::net::IpAddr;

use crate::{
    hook::{HandlerHookConfig, handler_fn},
    rt::Vetis,
};
use http::StatusCode;
use vetis::{VetisServer as _, VetisTestResult, host::HostConfig, server::ServerConfig};

#[tokio::test]
async fn test_vetis_add_host() -> VetisTestResult<()> {
    let handler_hook = HandlerHookConfig::builder()
        .handler(handler_fn(|_request| async move {
            let response = vetis::Response::builder()
                .status(StatusCode::OK)
                .text("Hello, world!");
            Ok(response)
        }))
        .build()?;

    let vhost_config = HostConfig::builder()
        .hostname("localhost")
        .root_directory("src/tests")
        .add_bind_address(
            "0.0.0.0"
                .parse::<IpAddr>()
                .unwrap(),
            0,
        )
        .add_hook(handler_hook)
        .build()?;

    let mut server = Vetis::builder()
        .add_host(vhost_config)
        .build();

    server
        .start()
        .await?;
    server
        .stop()
        .await?;

    Ok(())
}

#[tokio::test]
async fn test_vetis_start_no_hosts() -> VetisTestResult<()> {
    let mut server = Vetis::default();
    let result = server.start().await;
    assert!(result.is_err());
    Ok(())
}

#[tokio::test]
async fn test_vetis_stop_no_hosts() -> VetisTestResult<()> {
    assert!(
        Vetis::default()
            .stop()
            .await
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn test_vetis_from_config_reserves_ephemeral_port() -> VetisTestResult<()> {
    let host_config = HostConfig::builder()
        .hostname("localhost")
        .add_bind_address(
            "0.0.0.0"
                .parse::<IpAddr>()
                .unwrap(),
            0,
        )
        .build()?;
    let server_config = ServerConfig::builder()
        .add_host(host_config)
        .build()?;
    let server = Vetis::from_config(server_config).await?;

    assert_eq!(
        server
            .config()
            .hosts()
            .len(),
        1
    );
    Ok(())
}

#[tokio::test]
async fn test_vetis_add_multiple_hosts() -> VetisTestResult<()> {
    let mut server = Vetis::builder();

    for i in 0..3 {
        let handler_hook = HandlerHookConfig::builder()
            .handler(handler_fn(|_request| async move {
                let response = vetis::Response::builder()
                    .status(StatusCode::OK)
                    .text("Hello, world!");
                Ok(response)
            }))
            .build()?;

        let vhost_config = HostConfig::builder()
            .hostname(&format!("host{}", i))
            .root_directory("src/tests")
            .add_bind_address(
                "0.0.0.0"
                    .parse::<IpAddr>()
                    .unwrap(),
                8080,
            )
            .add_hook(handler_hook)
            .build()?;

        server = server.add_host(vhost_config);
    }

    Ok(())
}
