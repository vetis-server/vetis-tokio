use crate::{
    hook::{HandlerHookConfig, handler_fn},
    host::Host,
    tests::{CA_CERT, SERVER_CERT, SERVER_KEY},
};
use vetis::{VetisHosts, VetisTestResult, host::HostConfig, security::TlsConfig};

async fn create_test_hosts() -> VetisTestResult<VetisHosts<Host>> {
    let tls_config = TlsConfig::builder()
        .cert_file(SERVER_CERT)
        .key_file(SERVER_KEY)
        .ca_file(CA_CERT)
        .build()
        .expect("Failed to create security config");

    let host_config = HostConfig::builder()
        .hostname("localhost")
        .tls(tls_config)
        .add_hook(
            HandlerHookConfig::builder()
                .handler(handler_fn(|_request| async move {
                    let response = vetis::Response::builder()
                        .status(http::StatusCode::OK)
                        .text("Test response");
                    Ok(response)
                }))
                .build()
                .unwrap(),
        )
        .build()
        .expect("Failed to create host config");

    let host = Host::new(host_config).await?;

    let hosts = papaya::HashMap::new();
    hosts
        .pin_owned()
        .insert("localhost".into(), host.into());

    Ok(VetisHosts::new(hosts.into()))
}

async fn create_test_hosts_no_security() -> VetisTestResult<VetisHosts<Host>> {
    let host_config = HostConfig::builder()
        .hostname("localhost")
        .add_hook(
            HandlerHookConfig::builder()
                .handler(handler_fn(|_request| async move {
                    let response = vetis::Response::builder()
                        .status(http::StatusCode::OK)
                        .text("Test response");
                    Ok(response)
                }))
                .build()
                .unwrap(),
        )
        .build()
        .expect("Failed to create host config");

    let host = Host::new(host_config).await?;

    let hosts = papaya::HashMap::new();
    hosts
        .pin_owned()
        .insert("localhost".into(), host.into());
    Ok(VetisHosts::new(hosts.into()))
}

async fn create_test_hosts_invalid_key() -> VetisTestResult<VetisHosts<Host>> {
    let tls_config = TlsConfig::builder()
        .cert_file(SERVER_CERT)
        .key_file(SERVER_CERT)
        .build()
        .expect("Failed to create security config");

    let host_config = HostConfig::builder()
        .hostname("localhost")
        .tls(tls_config)
        .add_hook(
            HandlerHookConfig::builder()
                .handler(handler_fn(|_request| async move {
                    let response = vetis::Response::builder()
                        .status(http::StatusCode::OK)
                        .text("Test response");
                    Ok(response)
                }))
                .build()
                .unwrap(),
        )
        .build()
        .expect("Failed to create host config");

    let host = Host::new(host_config).await?;

    let hosts = papaya::HashMap::new();
    hosts
        .pin()
        .insert("localhost".into(), host.into());
    Ok(VetisHosts::new(hosts.into()))
}

#[tokio::test]
async fn test_tls_config_rejects_invalid_private_key() -> VetisTestResult<()> {
    let hosts = create_test_hosts_invalid_key().await?;

    assert!(crate::tls::TlsConfigResolver::create_tls_config(hosts).is_err());
    Ok(())
}
