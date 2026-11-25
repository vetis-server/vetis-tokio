#[cfg(feature = "http3")]
use crate::listener::Listener::Udp;
use crate::listener::tcp::TcpListener;
#[cfg(feature = "http3")]
use crate::listener::udp::UdpListener;
use crate::{host::Host, listener::Listener::Tcp};
use std::{
    fmt::{Display, Formatter},
    hash::Hash,
};
use vetis::{
    LogSender, VetisHosts, VetisResult,
    listener::{Listener as _, ListenerConfig},
    log::Logger,
};

pub(crate) mod tcp;
#[cfg(feature = "http3")]
pub(crate) mod udp;

/// Server listener enum
pub enum Listener {
    /// TCP listener
    Tcp(TcpListener),
    /// UDP listener
    #[cfg(feature = "http3")]
    Udp(UdpListener),
}

impl Display for Listener {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Tcp(tcp_listener) => f.write_fmt(format_args!(
                "{}:{}",
                tcp_listener
                    .config()
                    .interface(),
                tcp_listener
                    .config()
                    .port()
            )),
            #[cfg(feature = "http3")]
            Udp(udp_listener) => f.write_fmt(format_args!(
                "{}:{}",
                udp_listener
                    .config()
                    .interface(),
                udp_listener
                    .config()
                    .port()
            )),
        }
    }
}

impl Hash for Listener {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        match self {
            Tcp(listener) => listener.hash(state),
            #[cfg(feature = "http3")]
            Udp(listener) => listener.hash(state),
        }
    }
}

impl PartialEq for Listener {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Tcp(listener), Tcp(other_listener)) => listener == other_listener,
            #[cfg(feature = "http3")]
            (Udp(listener), Udp(other_listener)) => listener == other_listener,
            #[cfg(feature = "http3")]
            (Tcp(_), Udp(_)) | (Udp(_), Tcp(_)) => false,
        }
    }

    fn ne(&self, other: &Self) -> bool {
        match (self, other) {
            (Tcp(tcp_listener), Tcp(other_tcp_listener)) => tcp_listener != other_tcp_listener,
            #[cfg(feature = "http3")]
            (Udp(udp_listener), Udp(other_udp_listener)) => udp_listener != other_udp_listener,
            #[cfg(feature = "http3")]
            (Tcp(_), Udp(_)) | (Udp(_), Tcp(_)) => true,
        }
    }
}

impl Eq for Listener {}

impl From<TcpListener> for Listener {
    fn from(value: TcpListener) -> Self {
        Listener::Tcp(value)
    }
}

#[cfg(feature = "http3")]
impl From<UdpListener> for Listener {
    fn from(value: UdpListener) -> Self {
        Listener::Udp(value)
    }
}

impl vetis::listener::Listener for Listener {
    type RuntimeHost = Host;
    type Logger = Logger<LogSender>;

    fn logger(&mut self, logger: Self::Logger) {
        match self {
            Listener::Tcp(tcp_listener) => tcp_listener.logger(logger),
            #[cfg(feature = "http3")]
            Listener::Udp(udp_listener) => udp_listener.logger(logger),
        }
    }

    async fn reserve_port(&mut self) -> VetisResult<()> {
        match self {
            Listener::Tcp(tcp_listener) => {
                tcp_listener
                    .reserve_port()
                    .await
            }
            #[cfg(feature = "http3")]
            Listener::Udp(udp_listener) => {
                udp_listener
                    .reserve_port()
                    .await
            }
        }
    }

    fn config(&self) -> &ListenerConfig {
        match self {
            Listener::Tcp(tcp_listener) => tcp_listener.config(),
            #[cfg(feature = "http3")]
            Listener::Udp(udp_listener) => udp_listener.config(),
        }
    }

    fn reassign_port(&mut self, port: u16) {
        match self {
            Listener::Tcp(tcp_listener) => tcp_listener.reassign_port(port),
            #[cfg(feature = "http3")]
            Listener::Udp(udp_listener) => udp_listener.reassign_port(port),
        }
    }

    async fn listen(&mut self, hosts: VetisHosts<Self::RuntimeHost>) -> VetisResult<()> {
        match self {
            Listener::Tcp(tcp_listener) => {
                tcp_listener
                    .listen(hosts)
                    .await?
            }
            #[cfg(feature = "http3")]
            Listener::Udp(udp_listener) => {
                udp_listener
                    .listen(hosts)
                    .await?
            }
        }
        Ok(())
    }

    async fn stop(self) -> VetisResult<()> {
        match self {
            Listener::Tcp(tcp_listener) => {
                tcp_listener
                    .stop()
                    .await?
            }
            #[cfg(feature = "http3")]
            Listener::Udp(udp_listener) => {
                udp_listener
                    .stop()
                    .await?
            }
        }
        Ok(())
    }
}
