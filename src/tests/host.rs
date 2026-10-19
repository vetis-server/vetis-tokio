use crate::{
    host::{
        Host,
        path::{HandlerPath, handler_fn},
    },
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
        .build()
        .unwrap();

    let mut host = Host::new(config).await?;
    host.add_path(
        HandlerPath::builder()
            .uri("/")
            .handler(handler_fn(|_request, _ctx| async move {
                Ok(vetis::Response::builder()
                    .status(StatusCode::OK)
                    .text("Hello, world!"))
            }))
            .build()
            .unwrap(),
    );

    assert_eq!(
        host.config()
            .hostname(),
        "localhost"
    );

    Ok(())
}

#[tokio::test]
async fn test_host_tls_and_mutable_config() -> Result<(), Box<dyn std::error::Error>> {
    let config = HostConfig::builder()
        .hostname("before")
        .build()?;
    let mut host = Host::new(config).await?;
    let cert = tokio::fs::read(SERVER_CERT).await?;
    let key = tokio::fs::read(SERVER_KEY).await?;

    host.add_tls(vetis::security::Tls::from_cert_and_key(&cert, &key));
    assert!(host.tls().is_some());

    *host.config_mut() = HostConfig::builder()
        .hostname("after")
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
