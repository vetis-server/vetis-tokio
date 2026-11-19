use crate::{
    hook::{HandlerHook, handler_fn},
    host::Host,
    rt::Vetis,
};
use http::StatusCode;
use vetis::{
    VetisServer as _, VetisTestResult, host::HostConfig, listener::Listener, server::ServerConfig,
};

#[tokio::test]
async fn test_vetis_add_host() -> VetisTestResult<()> {
    let vhost_config = HostConfig::builder()
        .hostname("localhost")
        .root_directory("src/tests")
        .bind_addresses(&[(
            "0.0.0.0"
                .parse()
                .unwrap(),
            0,
        )])
        .build()?;

    let mut vhost = Host::new(vhost_config).await?;

    let handler_hook = HandlerHook::builder()
        .handler(handler_fn(|_request| async move {
            let response = vetis::Response::builder()
                .status(StatusCode::OK)
                .text("Hello, world!");
            Ok(response)
        }))
        .build()?;

    vhost.add_hook(handler_hook);

    let mut server = Vetis::builder()
        .add_host(vhost)
        .await?
        .build();

    let listener = server
        .listeners()
        .first()
        .unwrap();
    assert_eq!(listener.total_hosts(), 1);
    assert_ne!(
        listener
            .config()
            .port(),
        0
    );

    server.run().await?;
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
        .bind_addresses(&[(
            "0.0.0.0"
                .parse()
                .unwrap(),
            0,
        )])
        .build()?;
    let server_config = ServerConfig::builder()
        .add_host(host_config)
        .build()?;
    let server = Vetis::from_config(server_config).await?;

    assert_eq!(
        server
            .listeners()
            .len(),
        1
    );
    assert_ne!(
        server.listeners()[0]
            .config()
            .port(),
        0
    );
    Ok(())
}

#[tokio::test]
async fn test_vetis_add_multiple_hosts() -> VetisTestResult<()> {
    let mut server = Vetis::builder();

    for i in 0..3 {
        let vhost_config = HostConfig::builder()
            .hostname(&format!("host{}", i))
            .root_directory("src/tests")
            .bind_addresses(&[(
                "0.0.0.0"
                    .parse()
                    .unwrap(),
                8080,
            )])
            .build()?;

        let mut vhost = Host::new(vhost_config).await?;

        let handler_path = HandlerHook::builder()
            .handler(handler_fn(|_request| async move {
                let response = vetis::Response::builder()
                    .status(StatusCode::OK)
                    .text("Hello, world!");
                Ok(response)
            }))
            .build()?;

        vhost.add_hook(handler_path);

        server = server
            .add_host(vhost)
            .await?;
    }

    assert_eq!(
        server
            .build()
            .listeners()
            .first()
            .unwrap()
            .total_hosts(),
        3
    );

    Ok(())
}
