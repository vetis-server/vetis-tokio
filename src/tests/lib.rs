use crate::{
    host::{
        Host,
        path::{HandlerPath, handler_fn},
    },
    rt::Vetis,
};
use http::StatusCode;
use vetis::{Response, VetisServer as _, VetisTestResult, host::HostConfig, listener::Listener};

#[tokio::test]
async fn test_vetis_add_host() -> VetisTestResult<()> {
    let vhost_config = HostConfig::builder()
        .hostname("localhost")
        .root_directory("src/tests")
        .bind_addresses(&[(
            "0.0.0.0"
                .parse()
                .unwrap(),
            8080,
        )])
        .build()?;

    let mut vhost = Host::new(vhost_config).await?;

    let handler_path = HandlerPath::builder()
        .uri("/")
        .handler(handler_fn(|_request, _ctx| async move {
            Ok(Response::builder()
                .status(StatusCode::OK)
                .text("Hello, World!"))
        }))
        .build()?;

    vhost.add_path(handler_path);

    let server = Vetis::builder()
        .add_host(vhost)
        .await?;

    assert_eq!(
        server
            .listeners
            .first()
            .unwrap()
            .total_hosts(),
        1
    );

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

        let handler_path = HandlerPath::builder()
            .uri("/")
            .handler(handler_fn(|_request, _ctx| async move {
                Ok(Response::builder()
                    .status(StatusCode::OK)
                    .text("Hello, World!"))
            }))
            .build()?;

        vhost.add_path(handler_path);

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
