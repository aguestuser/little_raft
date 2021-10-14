use std::net::SocketAddr;

pub(crate) struct Gen {}

impl Gen {
    pub fn socket_addr() -> SocketAddr {
        let port = port_scanner::request_open_port().unwrap();
        SocketAddr::from(([127, 0, 0, 1], port))
    }
}
