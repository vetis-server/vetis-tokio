use crate::{
    host::{path::HandlerPath, Host},
    listener::build_listeners,
    rt::Vetis,
    tests::default_protocol_version,
};
use http::StatusCode;
use std::error::Error;
use vetis::{
    host::{handler_fn, HostConfig},
    listener::{Listener, ListenerConfig},
    server::ServerConfig,
    Response, VetisServer as _,
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
        .add_host(HostConfig::default())
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
        .add_host(HostConfig::default())
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
    let vhost_config = HostConfig::builder()
        .hostname("localhost")
        .root_directory("src/tests".into())
        .bind_addresses(vec![(
            "0.0.0.0"
                .parse()
                .unwrap(),
            8080,
        )])
        .build()?;

    let mut vhost = Host::new(vhost_config);

    let handler_path = HandlerPath::builder()
        .uri("/")
        .handler(handler_fn(|_request| async move {
            Ok(Response::builder()
                .status(StatusCode::OK)
                .text("Hello, World!"))
        }))
        .build()?;

    vhost.add_path(handler_path);

    let server = Vetis::builder()
        .add_listeners(build_listeners(create_listener()))?
        .add_host(vhost)?;

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
async fn test_vetis_start_no_hosts() -> Result<(), Box<dyn Error>> {
    let mut server = Vetis::default();
    let result = server.start().await;
    assert!(result.is_err());
    Ok(())
}

#[tokio::test]
async fn test_vetis_hosts() -> Result<(), Box<dyn Error>> {
    let vhost_config = HostConfig::builder()
        .hostname("localhost")
        .root_directory("src/tests".into())
        .bind_addresses(vec![(
            "0.0.0.0"
                .parse()
                .unwrap(),
            8080,
        )])
        .build()?;

    let mut vhost = Host::new(vhost_config);

    let handler_path = HandlerPath::builder()
        .uri("/")
        .handler(handler_fn(|_request| async move {
            Ok(Response::builder()
                .status(StatusCode::OK)
                .text("Hello, World!"))
        }))
        .build()?;

    vhost.add_path(handler_path);

    let server = Vetis::builder()
        .add_listeners(build_listeners(create_listener()))?
        .add_host(vhost)?
        .build();

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
async fn test_vetis_add_multiple_hosts() -> Result<(), Box<dyn Error>> {
    let mut server = Vetis::builder().add_listeners(build_listeners(create_listener()))?;

    for i in 0..3 {
        let vhost_config = HostConfig::builder()
            .hostname(&format!("host{}", i))
            .root_directory("src/tests".into())
            .bind_addresses(vec![(
                "0.0.0.0"
                    .parse()
                    .unwrap(),
                8080,
            )])
            .build()?;

        let mut vhost = Host::new(vhost_config);

        let handler_path = HandlerPath::builder()
            .uri("/")
            .handler(handler_fn(|_request| async move {
                Ok(Response::builder()
                    .status(StatusCode::OK)
                    .text("Hello, World!"))
            }))
            .build()?;

        vhost.add_path(handler_path);

        server = server.add_host(vhost)?;
    }

    assert_eq!(
        server
            .build()
            .listeners
            .first()
            .unwrap()
            .total_hosts(),
        3
    );

    Ok(())
}
