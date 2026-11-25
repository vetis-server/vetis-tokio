#![allow(unreachable_code, dead_code)]
use std::net::{IpAddr, Ipv4Addr};

use crate::hook::{HandlerFn, HandlerHookConfig, handler_fn};
use http::{StatusCode, Version};
use vetis::{host::HostConfig, security::TlsConfig};
use vetis_matchers::request::PathMatcherConfig;

mod host;
mod io;
mod lib;
mod listener;
#[cfg(target_os = "linux")]
mod server;
#[cfg(feature = "http3")]
mod service;
mod tls;

pub(crate) const CA_CERT: &str = "files/certs/ca.der";
pub(crate) const SERVER_CERT: &str = "files/certs/server.der";
pub(crate) const SERVER_KEY: &str = "files/certs/server.key.der";
pub(crate) const IP6_SERVER_CERT: &str = "files/certs/ip6-server.der";
pub(crate) const IP6_SERVER_KEY: &str = "files/certs/ip6-server.key.der";

pub(crate) const fn default_protocol_version() -> Version {
    #[cfg(not(any(feature = "http2", feature = "http3")))]
    return Version::HTTP_11;
    #[cfg(feature = "http2")]
    return Version::HTTP_2;
    #[cfg(feature = "http3")]
    return Version::HTTP_3;
}

fn create_hello_handler() -> HandlerFn {
    handler_fn(|_request| async move {
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
) -> Result<HostConfig, Box<dyn std::error::Error>> {
    let tls_config = TlsConfig::builder()
        .ca_file(ca)
        .cert_file(cert)
        .key_file(key)
        .build()?;

    let root_path = HandlerHookConfig::builder()
        .matcher(
            PathMatcherConfig::builder()
                .add_path("/hello*")
                .build()?,
        )
        .handler(handler)
        .build()?;

    let host_config = HostConfig::builder()
        .hostname(hostname)
        .root_directory(".")
        .tls(tls_config)
        .add_proto(default_protocol_version())
        .add_status_page(404, "files/public/404.html".into())
        .add_bind_address(Ipv4Addr::UNSPECIFIED, port)
        .add_hook(root_path)
        .build()?;

    Ok(host_config)
}
