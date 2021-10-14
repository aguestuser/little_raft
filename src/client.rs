use crate::error::Result;
use crate::node::State;
use crate::tcp::connection::Connection;
use dashmap::DashMap;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use std::hash::Hash;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpSocket;

pub struct Client {
    pub address: SocketAddr,
    pub server_addresses: Vec<SocketAddr>,
    pub state: State,
    pub connections: DashMap<String, Connection>,
}

impl Client {
    fn new(address: SocketAddr, server_addresses: Vec<SocketAddr>) -> Client {
        Self {
            address,
            server_addresses,
            state: State::New,
            connections: DashMap::new(),
        }
    }

    async fn run(&mut self) -> Result<()> {
        let server_addresses = self.server_addresses.clone();
        let connection_tuples: Vec<Result<(SocketAddr, Connection)>> =
            futures::future::try_join_all(server_addresses.into_iter().map(|addr| {
                tokio::spawn(async move {
                    let socket = TcpSocket::new_v4()?;
                    let stream = socket.connect(addr).await?;
                    Ok((addr, Connection::new(stream)))
                })
            }))
            .await?;
        connection_tuples.into_iter().for_each(|ct| {
            if let Ok((addr, conn)) = ct {
                let _ = self.connections.insert(addr.to_string(), conn);
            }
        });
        Ok(())
    }
}

#[cfg(test)]
mod test_client {
    use super::*;
    use crate::test_support::gen::Gen;
    use tokio::net::{TcpListener, TcpStream};
    use tokio::sync::mpsc;
    use tokio::sync::mpsc::Receiver;

    lazy_static! {
        static ref addr: SocketAddr = Gen::socket_addr();
        static ref server_addrs: Vec<SocketAddr> =
            vec![Gen::socket_addr(), Gen::socket_addr(), Gen::socket_addr()];
    }

    #[feature(async_closure)]
    async fn setup() -> Receiver<SocketAddr> {
        let (addr_tx, addr_rx) = mpsc::channel::<SocketAddr>(3);
        for i in 0..server_addrs.len() {
            let listener = TcpListener::bind(server_addrs[i]).await.unwrap();
            let tx = addr_tx.clone();
            tokio::spawn(async move {
                loop {
                    let (socket, client_addr) = listener.accept().await.unwrap();
                    tx.send(client_addr.clone()).await.unwrap();
                }
            });
        }
        return addr_rx;
    }

    #[tokio::test]
    async fn constructs_itself() {
        let client = Client::new(*addr, server_addrs.clone());

        assert_eq!(client.address, *addr);
        assert_eq!(client.server_addresses, server_addrs.clone());
        assert!(client.connections.is_empty());
    }

    #[tokio::test]
    async fn connects_to_servers() {
        let mut connected_addr_receiver = setup().await;
        let mut connected_addrs = Vec::<SocketAddr>::new();

        let mut client = Client::new(*addr, server_addrs.clone());
        client.run().await.unwrap();
        for _ in 0..server_addrs.len() {
            connected_addrs.push(connected_addr_receiver.recv().await.unwrap());
        }

        assert_eq!(connected_addrs.len(), server_addrs.len());
        assert_eq!(client.connections.len(), server_addrs.len())
    }
}
