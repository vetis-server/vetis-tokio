use crate::host::Host;
use crate::listener::Listener::Tcp;
#[cfg(feature = "http3")]
use crate::listener::Listener::Udp;
use crate::listener::tcp::TcpListener;
#[cfg(feature = "http3")]
use crate::listener::udp::UdpListener;
use std::hash::Hash;
use std::sync::Arc;
use vetis::LogSender;
use vetis::log::Logger;
use vetis::{VetisResult, listener::ListenerConfig};

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

    fn add_host(&mut self, host: Arc<Self::RuntimeHost>) -> VetisResult<()> {
        match self {
            Listener::Tcp(tcp_listener) => tcp_listener.add_host(host),
            #[cfg(feature = "http3")]
            Listener::Udp(udp_listener) => udp_listener.add_host(host),
        }
    }

    fn remove_host(&mut self, hostname: &str) -> VetisResult<()> {
        match self {
            Listener::Tcp(tcp_listener) => tcp_listener.remove_host(hostname),
            #[cfg(feature = "http3")]
            Listener::Udp(udp_listener) => udp_listener.remove_host(hostname),
        }
    }

    fn logger(&mut self, logger: Self::Logger) {
        match self {
            Listener::Tcp(tcp_listener) => tcp_listener.logger(logger),
            #[cfg(feature = "http3")]
            Listener::Udp(udp_listener) => udp_listener.logger(logger),
        }
    }

    /// Return total hosts count
    fn total_hosts(&self) -> usize {
        match self {
            Listener::Tcp(tcp_listener) => tcp_listener.total_hosts(),
            #[cfg(feature = "http3")]
            Listener::Udp(udp_listener) => udp_listener.total_hosts(),
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

    async fn listen(&mut self) -> VetisResult<()> {
        match self {
            Listener::Tcp(tcp_listener) => {
                tcp_listener
                    .listen()
                    .await?
            }
            #[cfg(feature = "http3")]
            Listener::Udp(udp_listener) => {
                udp_listener
                    .listen()
                    .await?
            }
        }
        Ok(())
    }

    async fn stop(&mut self) -> VetisResult<()> {
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
