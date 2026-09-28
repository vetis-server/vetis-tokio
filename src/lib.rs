#![doc = include_str!("../README.md")]
#![deny(missing_docs)]
#[cfg(all(feature = "http3", not(feature = "rust-tls")))]
compile_error!("http3 requires rust-tls!");

/// Host module
pub mod host;
/// IO module
pub mod io;
/// Listener module
pub mod listener;
/// Runtime module
pub mod rt;
/// Tests module
#[cfg(test)]
mod tests;
/// TLS module
mod tls;
/// Worker module
pub(crate) mod worker;

pub use crate::rt::Vetis;
pub use vetis::{
    VetisHosts,
    base::VetisServer,
    errors,
    host::HostConfig,
    listener::{Listener as VetisListener, ListenerConfig},
    request::Request,
    response::Response,
    security::{Tls, TlsConfig},
    server::ServerConfig,
};
