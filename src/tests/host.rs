use crate::host::{
    Host,
    path::{HandlerPath, handler_fn},
};
use http::StatusCode;
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
