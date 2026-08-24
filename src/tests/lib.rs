use crate::{
    host::{HostImpl, path::HandlerPath}, rt::Vetis, tests::default_protocol_version,
};
use http::StatusCode;
use std::error::Error;
use vetis::{
    Response, VetisServer as _, host::{HostConfig, handler_fn}, listener::ListenerConfig, server::ServerConfig,
};

fn create_listener() -> ListenerConfig {
    ListenerConfig::builder()
        .port(8080)
        .protos(vec![default_protocol_version()])
        .interface(
            "0.0.0.0"
                .parse()
                .unwrap(),
        )
        .build()
        .unwrap()
}

#[test]
fn test_vetis_new() {
    let config = ServerConfig::builder()
        .add_listener(create_listener())
        .build()
        .unwrap();
    let server = Vetis::new(config);

    assert_eq!(
        server
            .config()
            .listeners()
            .len(),
        1
    );
}

#[test]
fn test_vetis_config() {
    let config = ServerConfig::builder()
        .add_listener(create_listener())
        .build()
        .unwrap();

    let server = Vetis::new(config);

    assert_eq!(
        server
            .config()
            .listeners()
            .len(),
        1
    );
}

#[tokio::test]
async fn test_vetis_add_host() -> Result<(), Box<dyn Error>> {
    let config = ServerConfig::builder()
        .add_listener(create_listener())
        .build()
        .unwrap();
    let mut server = Vetis::new(config);

    let vhost_config = HostConfig::builder()
        .hostname("localhost")
        .root_directory("src/tests".into())
        .build()?;

    let mut vhost = HostImpl::new(vhost_config);

    let handler_path = HandlerPath::builder()
        .uri("/")
        .handler(handler_fn(|_request| async move {
            Ok(Response::builder()
                .status(StatusCode::OK)
                .text("Hello, World!"))
        }))
        .build()?;

    vhost.add_path(handler_path);

    server
        .add_host(vhost)
        .await;

    assert_eq!(
        server
            .hosts()
            .read()
            .await
            .len(),
        1
    );

    Ok(())
}

#[tokio::test]
async fn test_vetis_start_no_hosts() -> Result<(), Box<dyn Error>> {
    let config = ServerConfig::builder()
        .add_listener(create_listener())
        .build()
        .unwrap();
    let mut server = Vetis::new(config);

    let result = server.start().await;

    assert!(result.is_err());

    Ok(())
}

#[tokio::test]
async fn test_vetis_hosts() -> Result<(), Box<dyn Error>> {
    let config = ServerConfig::builder()
        .add_listener(create_listener())
        .build()
        .unwrap();
    let mut server = Vetis::new(config);

    let vhost_config = HostConfig::builder()
        .hostname("localhost")
        .root_directory("src/tests".into())
        .build()?;

    let mut vhost = HostImpl::new(vhost_config);

    let handler_path = HandlerPath::builder()
        .uri("/")
        .handler(handler_fn(|_request| async move {
            Ok(Response::builder()
                .status(StatusCode::OK)
                .text("Hello, World!"))
        }))
        .build()?;

    vhost.add_path(handler_path);

    server
        .add_host(vhost)
        .await;

    let hosts = server
        .hosts()
        .read()
        .await;
    assert_eq!(hosts.len(), 1);

    Ok(())
}

#[tokio::test]
async fn test_vetis_add_multiple_hosts() -> Result<(), Box<dyn Error>> {
    let config = ServerConfig::builder()
        .add_listener(create_listener())
        .build()
        .unwrap();
    let mut server = Vetis::new(config);

    for i in 0..3 {
        let vhost_config = HostConfig::builder()
            .hostname(&format!("host{}", i))
            .root_directory("src/tests".into())
            .build()?;

        let mut vhost = HostImpl::new(vhost_config);

        let handler_path = HandlerPath::builder()
            .uri("/")
            .handler(handler_fn(|_request| async move {
                Ok(Response::builder()
                    .status(StatusCode::OK)
                    .text("Hello, World!"))
            }))
            .build()?;

        vhost.add_path(handler_path);

        server
            .add_host(vhost)
            .await;
    }

    assert_eq!(
        server
            .hosts()
            .read()
            .await
            .len(),
        3
    );

    Ok(())
}
