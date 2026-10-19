use crate::listener::tcp::TcpListener;
use std::net::{IpAddr, Ipv4Addr};
use vetis::listener::ListenerConfig;

fn listener(port: u16, interface: IpAddr) -> TcpListener {
    TcpListener::new(
        ListenerConfig::builder()
            .interface(interface)
            .port(port)
            .build()
            .unwrap(),
    )
}

#[test]
fn inequality_detects_port_or_interface_changes() {
    let original = listener(8080, Ipv4Addr::UNSPECIFIED.into());

    assert!(original != listener(8081, Ipv4Addr::UNSPECIFIED.into()));
    assert!(original != listener(8080, Ipv4Addr::LOCALHOST.into()));
}
