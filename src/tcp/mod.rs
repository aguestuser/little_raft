use std::net::SocketAddr;

pub mod client;
pub mod connection;
pub mod envelope;

pub const REQUEST_BUFFER_SIZE: usize = 16;

#[derive(Clone)]
pub struct ServerConfig {
    pub address: SocketAddr,
}
