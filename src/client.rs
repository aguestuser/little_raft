use crate::error::{IllegalStateError, Result};
use crate::node::State;
use crate::tcp::connection::Connection;
use dashmap::DashMap;
use futures::StreamExt;
use std::net::SocketAddr;
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

    async fn write(&mut self, server_addr: &str, msg: Vec<u8>) -> Result<()> {
        if let Some(mut conn) = self.connections.get_mut(server_addr) {
            conn.write(msg).await
        } else {
            Err(Box::new(IllegalStateError::NoPeerAtAddress(
                server_addr.to_string(),
            )))
        }
    }
}

#[cfg(test)]
mod test_client {
    use super::*;
    use crate::test_support::gen::Gen;
    use tokio::net::{TcpListener, TcpStream};
    use tokio::sync::mpsc;
    use tokio::sync::mpsc::Receiver;

    struct Runner {
        client_addr: SocketAddr,
        server_addrs: Vec<SocketAddr>,
        addr_rx: Receiver<SocketAddr>,
        msg_rx: Receiver<(SocketAddr, Vec<u8>)>,
    }

    #[feature(async_closure)]
    async fn setup() -> Runner {
        let buf_size = 10;
        let client_addr: SocketAddr = Gen::socket_addr();
        let server_addrs: Vec<SocketAddr> =
            vec![Gen::socket_addr(), Gen::socket_addr(), Gen::socket_addr()];

        let (addr_tx, addr_rx) = mpsc::channel::<SocketAddr>(buf_size);
        let (msg_tx, msg_rx) = mpsc::channel::<(SocketAddr, Vec<u8>)>(buf_size);

        for server_addr in server_addrs.clone().into_iter() {
            let listener = TcpListener::bind(server_addr).await.unwrap();
            let addr_tx = addr_tx.clone();
            let msg_tx = msg_tx.clone();

            tokio::spawn(async move {
                loop {
                    let (socket, client_addr) = listener.accept().await.unwrap();
                    addr_tx.send(client_addr.clone()).await.unwrap();
                    let msg_tx = msg_tx.clone();

                    tokio::spawn(async move {
                        let mut conn = Connection::new(socket);
                        loop {
                            let msg = conn.read().await.unwrap();
                            msg_tx.send((server_addr, msg)).await.unwrap();
                        }
                    });
                }
            });
        }

        return Runner {
            client_addr,
            server_addrs,
            addr_rx,
            msg_rx,
        };
    }

    #[tokio::test]
    async fn constructs_itself() {
        let client_addr = Gen::socket_addr();
        let server_addrs = vec![Gen::socket_addr(), Gen::socket_addr(), Gen::socket_addr()];
        let client = Client::new(client_addr, server_addrs.clone());

        assert_eq!(client.address, client_addr);
        assert_eq!(client.server_addresses, server_addrs.clone());
        assert!(client.connections.is_empty());
    }

    #[tokio::test]
    async fn connects_to_servers() {
        let Runner {
            client_addr,
            server_addrs,
            mut addr_rx,
            ..
        } = setup().await;
        let mut connected_addrs = Vec::<SocketAddr>::new();

        let mut client = Client::new(client_addr, server_addrs.clone());
        client.run().await.unwrap();
        for _ in 0..server_addrs.len() {
            connected_addrs.push(addr_rx.recv().await.unwrap());
        }

        assert_eq!(connected_addrs.len(), server_addrs.len());
        assert_eq!(client.connections.len(), server_addrs.len())
    }

    #[tokio::test]
    async fn writes_to_a_server() {
        let Runner {
            client_addr,
            server_addrs,
            mut addr_rx,
            mut msg_rx,
            ..
        } = setup().await;

        let mut client = Client::new(client_addr, server_addrs.clone());
        client.run().await.unwrap();
        for _ in 0..2 {
            let _ = addr_rx.recv().await.unwrap();
        }

        let _ = client
            .write(&server_addrs[0].to_string(), b"hello".to_vec())
            .await;
        let (a, msg) = msg_rx.recv().await.unwrap();

        assert_eq!(msg, b"hello\n".to_vec());
    }
}
