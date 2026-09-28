use crate::{
    host::{
        Host,
        path::{HandlerPath, handler_fn},
    },
    tests::{CA_CERT, SERVER_CERT, SERVER_KEY},
};
use vetis::{
    VetisHosts, VetisTestResult, errors::VetisError, host::HostConfig, security::TlsConfig,
};

async fn create_test_hosts() -> VetisTestResult<VetisHosts<Host>> {
    let security_config = TlsConfig::builder()
        .cert_file(SERVER_CERT)
        .key_file(SERVER_KEY)
        .ca_file(CA_CERT)
        .build()
        .expect("Failed to create security config");

    let host_config = HostConfig::builder()
        .hostname("localhost")
        .tls(security_config)
        .build()
        .expect("Failed to create host config");

    let mut host = Host::new(host_config).await?;
    host.add_path(
        HandlerPath::builder()
            .uri("/")
            .handler(handler_fn(|_req, _ctx| async move {
                Ok::<_, VetisError>(
                    vetis::Response::builder()
                        .status(http::StatusCode::OK)
                        .text("Test response"),
                )
            }))
            .build()
            .unwrap(),
    );

    let hosts = papaya::HashMap::new();
    hosts
        .pin_owned()
        .insert("localhost".into(), host.into());

    Ok(VetisHosts::new(hosts.into()))
}

async fn create_test_hosts_no_security() -> VetisTestResult<VetisHosts<Host>> {
    let host_config = HostConfig::builder()
        .hostname("localhost")
        .build()
        .expect("Failed to create host config");

    let mut host = Host::new(host_config).await?;
    host.add_path(
        HandlerPath::builder()
            .uri("/")
            .handler(handler_fn(|_req, _ctx| async move {
                Ok::<_, VetisError>(
                    vetis::Response::builder()
                        .status(http::StatusCode::OK)
                        .text("Test response"),
                )
            }))
            .build()
            .unwrap(),
    );

    let hosts = papaya::HashMap::new();
    hosts
        .pin_owned()
        .insert("localhost".into(), host.into());
    Ok(VetisHosts::new(hosts.into()))
}

async fn create_test_hosts_invalid_key() -> VetisTestResult<VetisHosts<Host>> {
    let security_config = TlsConfig::builder()
        .cert_file(SERVER_CERT)
        .key_file(SERVER_KEY) // Invalid key
        .build()
        .expect("Failed to create security config");

    let host_config = HostConfig::builder()
        .hostname("localhost")
        .tls(security_config)
        .build()
        .expect("Failed to create host config");

    let mut host = Host::new(host_config).await?;
    host.add_path(
        HandlerPath::builder()
            .uri("/")
            .handler(handler_fn(|_req, _ctx| async move {
                Ok::<_, VetisError>(
                    vetis::Response::builder()
                        .status(http::StatusCode::OK)
                        .text("Test response"),
                )
            }))
            .build()
            .unwrap(),
    );

    let hosts = papaya::HashMap::new();
    hosts
        .pin()
        .insert("localhost".into(), host.into());
    Ok(VetisHosts::new(hosts.into()))
}
