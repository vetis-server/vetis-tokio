use crate::host::Host;
use crate::listener::tcp::TcpListener;
#[cfg(feature = "http3")]
use crate::listener::udp::UdpListener;
use http::Version;
use std::sync::Arc;
use vetis::{listener::ListenerConfig, VetisResult};

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

/// Build listeners out of ListenerConfig
pub fn build_listeners(config: ListenerConfig) -> Vec<Listener> {
    let mut listeners = Vec::new();
    for proto in config.protos() {
        match proto {
            &Version::HTTP_11 | &Version::HTTP_2 => {
                listeners.push(Listener::Tcp(TcpListener::new(config.clone())));
            }
            #[cfg(feature = "http3")]
            &Version::HTTP_3 => {
                listeners.push(Listener::Udp(UdpListener::new(config.clone())));
            }
            _ => {}
        }
    }
    listeners
}

impl vetis::listener::Listener for Listener {
    type RuntimeHost = Host;

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

    /// Return total hosts count
    fn total_hosts(&self) -> usize {
        match self {
            Listener::Tcp(tcp_listener) => tcp_listener.total_hosts(),
            #[cfg(feature = "http3")]
            Listener::Udp(udp_listener) => udp_listener.total_hosts(),
        }
    }

    /// Ask OS to reserve a free port
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
