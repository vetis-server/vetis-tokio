#![allow(unreachable_code, dead_code)]
use http::{StatusCode, Version};
use vetis::{host::HostConfig, security::TlsConfig};
use vetis_macros::status_pages;

use crate::host::{
    Host,
    path::{HandlerFn, HandlerPath, handler_fn},
};

mod host;
mod io;
mod lib;
mod listener;
mod paths;
#[cfg(target_os = "linux")]
mod server;
mod tls;

pub(crate) const CA_CERT: &str = "certs/ca.der";
pub(crate) const SERVER_CERT: &str = "certs/server.der";
pub(crate) const SERVER_KEY: &str = "certs/server.key.der";
pub(crate) const IP6_SERVER_CERT: &str = "certs/ip6-server.der";
pub(crate) const IP6_SERVER_KEY: &str = "certs/ip6-server.key.der";

pub(crate) const fn default_protocol_version() -> Version {
    #[cfg(not(any(feature = "http2", feature = "http3")))]
    return Version::HTTP_11;
    #[cfg(feature = "http2")]
    return Version::HTTP_2;
    #[cfg(feature = "http3")]
    return Version::HTTP_3;
}

fn create_hello_handler() -> HandlerFn {
    handler_fn(|_request, _ctx| async move {
        let response = vetis::Response::builder()
            .status(StatusCode::OK)
            .text("Hello from localhost");
        Ok(response)
    })
}

async fn create_host(
    handler: HandlerFn,
    hostname: &str,
    port: u16,
    ca: &str,
    cert: &str,
    key: &str,
) -> Result<Host, Box<dyn std::error::Error>> {
    let security_config = TlsConfig::builder()
        .ca_file(ca)
        .cert_file(cert)
        .key_file(key)
        .build()?;

    let host_config = HostConfig::builder()
        .hostname(hostname)
        .root_directory(".")
        .protos(&[default_protocol_version()])
        .tls(security_config)
        .status_pages(status_pages! {
            404 @ "files/404.html"
        })
        .bind_addresses(&[(
            "0.0.0.0"
                .parse()
                .unwrap(),
            port,
        )])
        .build()?;

    let root_path = HandlerPath::builder()
        .uri("/hello")
        .handler(handler)
        .build()?;

    let mut host = Host::new(host_config).await?;

    host.add_path(root_path);

    Ok(host)
}
