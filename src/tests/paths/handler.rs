use crate::{
    host::{
        Host,
        path::{HandlerPath, handler_fn},
    },
    tests::{CA_CERT, SERVER_CERT, SERVER_KEY, default_protocol_version},
};
use deboa::{
    cert::{CertificateExt, ContentEncoding},
    request,
};
use deboa_tokio::{Client, cert::DeboaCertificate};
use http::StatusCode;
use rand::random_range;
use vetis::{VetisServer as _, host::HostConfig, security::TlsConfig};

#[tokio::test]
async fn test_handler() -> Result<(), Box<dyn std::error::Error>> {
    let port = random_range(9000..=20000);

    let security_config = TlsConfig::builder()
        .ca_file(CA_CERT)
        .cert_file(SERVER_CERT)
        .key_file(SERVER_KEY)
        .build()?;

    let host_config = HostConfig::builder()
        .hostname("localhost")
        .root_directory(".")
        .protos(&[default_protocol_version()])
        .tls(security_config)
        .bind_addresses(&[(
            "0.0.0.0"
                .parse()
                .unwrap(),
            port,
        )])
        .build()?;

    let root_path = HandlerPath::builder()
        .uri("/hello")
        .handler(handler_fn(|_request, _ctx| async move {
            let response = vetis::Response::builder()
                .status(StatusCode::OK)
                .text("Hello from localhost");
            Ok(response)
        }))
        .build()?;

    let mut host = Host::new(host_config).await?;

    host.add_path(root_path);

    let mut server = crate::Vetis::builder()
        .add_host(host)
        .await?
        .build();

    server
        .start()
        .await?;

    let cert = tokio::fs::read(CA_CERT).await?;
    let client = Client::builder()
        .certificate(DeboaCertificate::from_slice(&cert, ContentEncoding::DER))
        .build();

    let request = request::get(format!("https://localhost:{}{}", port, "/hello"))?
        .send_with(&client)
        .await?;

    assert_eq!(request.status(), StatusCode::OK);

    server
        .stop()
        .await?;

    Ok(())
}
