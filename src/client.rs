use crate::error::{IllegalStateError, Result};
use crate::node::State;
use crate::tcp::connection::Connection;
use dashmap::DashMap;

use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpSocket;
use tokio::sync::Mutex;

pub struct Client {
    pub address: SocketAddr,
    pub server_addresses: Vec<SocketAddr>,
    pub state: State,
    // TODO: might need to be an Arc<Connection> to parallelize!
    pub connections: DashMap<String, Arc<Mutex<Connection>>>,
}

impl Client {
    pub fn new(address: SocketAddr, server_addresses: Vec<SocketAddr>) -> Client {
        Self {
            address,
            server_addresses,
            state: State::New,
            connections: DashMap::new(),
        }
    }

    pub async fn run(&mut self) -> Result<()> {
        let server_addresses = self.server_addresses.clone();
        let connection_tuples: Vec<Result<(SocketAddr, Arc<Mutex<Connection>>)>> =
            futures::future::try_join_all(server_addresses.into_iter().map(|addr| {
                tokio::spawn(async move {
                    let socket = TcpSocket::new_v4()?;
                    let stream = socket.connect(addr).await?;
                    Ok((addr, Arc::new(Mutex::new(Connection::new(stream)))))
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

    pub async fn write(&mut self, peer_addr: &String, msg: &Vec<u8>) -> Result<()> {
        if let Some(conn_arc) = self.connections.get(peer_addr) {
            let conn_arc = conn_arc.clone();
            let mut conn = conn_arc.lock().await;
            conn.write(msg).await
        } else {
            Err(Box::new(IllegalStateError::NoPeerAtAddress(
                peer_addr.to_string(),
            )))
        }
    }

    pub async fn write_many(&mut self, peer_addrs: &Vec<String>, msg: &Vec<u8>) -> Result<Vec<()>> {
        let connections = peer_addrs
            .into_iter()
            .map(|peer_addr| {
                // TODO: don't use unwrap here!
                let c_arc = self.connections.get(peer_addr).unwrap();
                c_arc.clone()
            })
            .collect::<Vec<Arc<Mutex<Connection>>>>();

        let results: Vec<Result<()>> =
            futures::future::try_join_all(connections.into_iter().map(|c_arc| {
                let c_arc = c_arc.clone();
                let msg = msg.clone();
                tokio::spawn(async move {
                    let mut c = c_arc.lock().await;
                    c.write(&msg).await
                })
            }))
            .await?;
        results.into_iter().collect()
    }

    pub async fn broadcast(&mut self, msg: &Vec<u8>) -> Result<Vec<()>> {
        let peer_addrs = self
            .connections
            .iter()
            .map(|entry| entry.key().to_string())
            .collect::<Vec<String>>();
        self.write_many(&peer_addrs, msg).await
    }
}

/*********
 * TESTS *
 *********/

#[cfg(test)]
mod test_client {
    use super::*;
    use crate::tcp::NEWLINE;
    use crate::test_support::gen::Gen;
    use std::collections::HashSet;
    use std::iter::FromIterator;
    use tokio::net::TcpListener;
    use tokio::sync::mpsc;
    use tokio::sync::mpsc::Receiver;

    struct Runner {
        client_addr: SocketAddr,
        server_addrs: Vec<SocketAddr>,
        conn_rx: Receiver<SocketAddr>,
        msg_rx: Receiver<(SocketAddr, Vec<u8>)>,
    }

    async fn setup() -> Runner {
        let buf_size = 10;
        let client_addr: SocketAddr = Gen::socket_addr();
        let server_addrs: Vec<SocketAddr> =
            vec![Gen::socket_addr(), Gen::socket_addr(), Gen::socket_addr()];

        let (conn_tx, conn_rx) = mpsc::channel::<SocketAddr>(buf_size);
        let (msg_tx, msg_rx) = mpsc::channel::<(SocketAddr, Vec<u8>)>(buf_size);

        for server_addr in server_addrs.clone().into_iter() {
            let listener = TcpListener::bind(server_addr).await.unwrap();
            let conn_tx = conn_tx.clone();
            let msg_tx = msg_tx.clone();

            tokio::spawn(async move {
                loop {
                    let (socket, client_addr) = listener.accept().await.unwrap();
                    // println!("> Peer listening at {:?}", server_addr);

                    conn_tx.send(client_addr.clone()).await.unwrap();
                    let msg_tx = msg_tx.clone();

                    tokio::spawn(async move {
                        let mut conn = Connection::new(socket);
                        loop {
                            let msg = conn.read().await.unwrap();
                            // println!("> Peer at {:?} got msg: {:?}", server_addr, msg);
                            msg_tx.send((server_addr, msg)).await.unwrap();
                        }
                    });
                }
            });
        }

        return Runner {
            client_addr,
            server_addrs,
            conn_rx,
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
            mut conn_rx,
            ..
        } = setup().await;
        let mut connected_addrs = Vec::<SocketAddr>::new();

        let mut client = Client::new(client_addr, server_addrs.clone());
        client.run().await.unwrap();
        for _ in 0..server_addrs.len() {
            connected_addrs.push(conn_rx.recv().await.unwrap());
        }

        assert_eq!(connected_addrs.len(), server_addrs.len());
        assert_eq!(client.connections.len(), server_addrs.len())
    }

    #[tokio::test]
    async fn writes_to_a_peer() {
        let Runner {
            client_addr,
            server_addrs,
            mut conn_rx,
            mut msg_rx,
            ..
        } = setup().await;

        let mut client = Client::new(client_addr, server_addrs.clone());
        client.run().await.unwrap();
        for _ in 0..2 {
            let _ = conn_rx.recv().await.unwrap();
        }

        let _ = client
            .write(&server_addrs[0].to_string(), &b"hello".to_vec())
            .await;
        let (peer_addr, msg) = msg_rx.recv().await.unwrap();

        assert_eq!(peer_addr, server_addrs[0]);
        assert_eq!(msg, b"hello\n".to_vec());
    }

    #[tokio::test]
    async fn broadcasts_to_all_peers() {
        let Runner {
            client_addr,
            server_addrs,
            mut conn_rx,
            mut msg_rx,
            ..
        } = setup().await;

        let mut client = Client::new(client_addr, server_addrs.clone());
        client.run().await.unwrap();
        for _ in 0..2 {
            let _ = conn_rx.recv().await.unwrap();
        }

        let msg = &b"hello".to_vec();
        let _ = client.broadcast(msg).await.unwrap();
        let (peer_1, msg_1) = msg_rx.recv().await.unwrap();
        let (peer_2, msg_2) = msg_rx.recv().await.unwrap();
        let (peer_3, msg_3) = msg_rx.recv().await.unwrap();

        let delimited_msg: Vec<u8> = [msg.clone(), vec![NEWLINE]].concat();
        assert_eq!(
            vec![&msg_1, &msg_2, &msg_3],
            vec![&delimited_msg, &delimited_msg, &delimited_msg]
        );
        assert_eq!(
            HashSet::<_>::from_iter(vec![peer_1, peer_2, peer_3].into_iter()),
            HashSet::<_>::from_iter(server_addrs.into_iter()),
        );
    }

    #[tokio::test]
    async fn writes_to_many_peers() {
        let msg = &b"hello".to_vec();
        let delimited_msg: Vec<u8> = [msg.clone(), vec![NEWLINE]].concat();

        let Runner {
            client_addr,
            server_addrs,
            mut conn_rx,
            mut msg_rx,
            ..
        } = setup().await;

        let mut client = Client::new(client_addr, server_addrs.clone());
        client.run().await.unwrap();
        for _ in 0..2 {
            let _ = conn_rx.recv().await.unwrap();
        }

        let recipient_addrs = server_addrs[0..2]
            .iter()
            .map(|x| x.to_string())
            .collect::<Vec<String>>();

        let _ = client.write_many(&recipient_addrs, msg).await.unwrap();
        let (peer_1, msg_1) = msg_rx.recv().await.unwrap();
        let (peer_2, msg_2) = msg_rx.recv().await.unwrap();

        assert_eq!(vec![&msg_1, &msg_2], vec![&delimited_msg, &delimited_msg],);
        assert_eq!(
            HashSet::<_>::from_iter(vec![peer_1.to_string(), peer_2.to_string()].into_iter()),
            HashSet::<_>::from_iter(recipient_addrs.into_iter()),
        );
    }
}
