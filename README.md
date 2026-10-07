# VeTiS Tokio (Very Tiny Server with tokio runtime support)

[![Crates.io downloads](https://img.shields.io/crates/d/vetis-tokio)](https://crates.io/crates/vetis-tokio) [![crates.io](https://img.shields.io/crates/v/vetis-tokio?style=flat-square)](https://crates.io/crates/vetis-tokio) [![Build Status](https://github.com/vetis-server/vetis-tokio/actions/workflows/rust.yml/badge.svg?event=push)](https://github.com/vetis-server/vetis-tokio/actions/workflows/rust.yml) ![Crates.io MSRV](https://img.shields.io/crates/msrv/vetis-tokio) [![Documentation](https://docs.rs/vetis-tokio/badge.svg)](https://docs.rs/vetis-tokio/latest/vetis-tokio) [![MIT licensed](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/vetis-server/vetis-tokio/blob/main/LICENSE.md)  [![codecov](https://codecov.io/gh/vetis-server/vetis-tokio/graph/badge.svg?token=T0HSBAPVSI)](https://codecov.io/gh/vetis-server/vetis-tokio)

## Quick Start

Add VeTiS to your `Cargo.toml`:

```toml
vetis = { version = "0.1.0" }
```

## Crate features

- http2
- http3
- rust-tls (default)

## External crates

- vetis-static
- vetis-rev-proxy
- vetis-fash
- vetis-log
- auth

## Usage Example

Here's how simple it is to create a web server with VeTiS:

```rust, no_run
use http::Version;
use hyper::StatusCode;
use vetis::{
    security::TlsConfig,
    server::{ServerConfig},
    host::{handler_fn, HostConfig},
};
use vetis_macros::status_pages;
use vetis_tokio::{
    host::{path::HandlerPath, Host},
    Vetis, VetisServer as _
};

pub(crate) const CA_CERT: &str = "certs/ca.der";
pub(crate) const SERVER_CERT: &str = "certs/server.der";
pub(crate) const SERVER_KEY: &str = "certs/server.key.der";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::Builder::from_env(env_logger::Env::default().filter_or("RUST_LOG", "error")).init();
d on deboa-tests for testing purposes, it turned into a complete http server project, the goal is make it very flexible, while keeping it small and fast.
    let security_config = TlsConfig::builder()
        .ca_file(CA_CERT)
        .cert_file(SERVER_CERT)
        .key_file(SERVER_KEY)
        .build()?;

    let localhost_config = HostConfig::builder()
        .hostname("localhost")
        .tls(security_config)
        .root_directory("/home/rogerio/Downloads")
        .bind_addresses(vec![(
            "0.0.0.0"
                .parse()
                .unwrap(),
            8443,
        )])
        .status_pages(status_pages! {
            404 @ "404.html",
            500 @ "500.html",
        })
        .build()?;

    let mut localhost_host = Host::new(localhost_config).await?;

    let root_path = HandlerPath::builder()
        .uri("/hello")
        .handler(handler_fn(|_request| async move {
            let response = vetis::Response::builder()
                .status(StatusCode::OK)
                .text("Hello from localhost");
            Ok(response)
        }))
        .build()?;

    localhost_host.add_path(root_path);

    let health_path = HandlerPath::builder()
        .uri("/health")
        .handler(handler_fn(|_request| async move {
            let response = vetis::Response::builder()
                .status(StatusCode::OK)
                .text("Health check");
            Ok(response)
        }))
        .build()?;

    localhost_host.add_path(health_path);

    let mut server = Vetis::builder()
        .add_host(localhost_host)?
        .build();

    server.run().await?;

    server
        .stop()
        .await?;

    Ok(())
}
```

## License

Licensed under either of

- Apache License, Version 2.0
  (LICENSE-APACHE or <https://www.apache.org/licenses/LICENSE-2.0>)
- MIT license
  (LICENSE-MIT or <https://opensource.org/licenses/MIT>)

at your option.

## Author

Rogerio Pereira Araujo <rogerio.araujo@gmail.com>
