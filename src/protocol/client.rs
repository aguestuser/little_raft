use std::net::SocketAddr;
use std::sync::Arc;

use dashmap::DashMap;
use futures::StreamExt;
use tokio::net::TcpSocket;

use crate::error::{AsyncError, IllegalStateError, Result};
use crate::node::State;
use crate::protocol::connection::ClientConnection;
use crate::protocol::request::Request;

pub struct Client {
    pub address: SocketAddr,
    pub peer_addresses: Vec<SocketAddr>,
    pub state: State,
    pub peers: DashMap<String, Peer>,
}

pub struct Peer {
    address: SocketAddr, // TODO: should this be a String?
    connection: Arc<ClientConnection>,
}

impl Client {
    pub fn new(address: SocketAddr, peer_addresses: Vec<SocketAddr>) -> Client {
        Self {
            address,
            peer_addresses,
            state: State::New,
            peers: DashMap::new(),
        }
    }

    pub async fn run(&mut self) -> Result<()> {
        let peers: Vec<Result<Peer>> =
            futures::future::try_join_all(self.peer_addresses.iter().map(|&address| {
                tokio::spawn(async move {
                    let socket = TcpSocket::new_v4()?;
                    let stream = socket.connect(address).await?;
                    let connection = Arc::new(ClientConnection::new(stream));
                    Ok(Peer {
                        address,
                        connection,
                    })
                })
            }))
            .await?;

        peers.into_iter().for_each(|ct| {
            // TODO: try to perform this insertion in body of `try_join_all` (blocker: Error types!)
            if let Ok(peer) = ct {
                self.peers.insert(peer.address.to_string(), peer);
            }
        });

        Ok(())
    }

    async fn write(connection: Arc<ClientConnection>, req: Request) -> Result<()> {
        connection.write(&req).await
    }

    pub async fn write_one(&mut self, peer_addr: &String, req: &Request) -> Result<()> {
        if let Some(peer) = self.peers.get(peer_addr) {
            Self::write(peer.connection.clone(), req.clone()).await
        } else {
            Err(Box::new(IllegalStateError::NoPeerAtAddress(
                peer_addr.to_string(),
            )))
        }
    }

    pub async fn write_many(&mut self, peer_addrs: &Vec<String>, req: &Request) -> Vec<Result<()>> {
        let num_writes = peer_addrs.len();

        let peers_by_address = peer_addrs
            .clone()
            .into_iter()
            .map(|peer_addr| (self.peers.get(&peer_addr), peer_addr));

        let writes =
            futures::stream::iter(peers_by_address.map(|(peer_entry, addr)| match peer_entry {
                Some(peer) => tokio::spawn(Self::write(peer.connection.clone(), req.clone())),
                None => tokio::spawn(async move {
                    // TODO: can we avoid spawning here since we know we don't want to perform work?
                    //  blocker: matching `Error` types!
                    Err(
                        Box::new(IllegalStateError::NoPeerAtAddress(addr.to_string()))
                            as AsyncError,
                    )
                }),
            }));

        writes
            .buffer_unordered(num_writes)
            .map(|r| r.unwrap_or_else(|e| Err(e.into()))) // un-nest Result<Result>
            .collect::<Vec<Result<()>>>()
            .await
    }

    pub async fn broadcast(&mut self, req: &Request) -> Vec<Result<()>> {
        let peer_addrs = self
            .peers
            .iter()
            .map(|entry| entry.key().to_string())
            .collect::<Vec<String>>();
        self.write_many(&peer_addrs, req).await
    }
}

/*********
 * TESTS *
 *********/

#[cfg(test)]
mod test_client {
    use std::collections::HashSet;
    use std::iter::FromIterator;

    use tokio::net::TcpListener;
    use tokio::sync::mpsc;
    use tokio::sync::mpsc::Receiver;

    use crate::protocol::connection::ServerConnection;
    use crate::test_support::gen::Gen;

    use super::*;

    struct Runner {
        client_addr: SocketAddr,
        server_addrs: Vec<SocketAddr>,
        conn_rx: Receiver<SocketAddr>,
        req_rx: Receiver<(SocketAddr, Request)>,
    }

    lazy_static! {
        static ref REQ: Request = Request::Get {
            id: 42,
            key: "foo".to_string()
        };
    }

    async fn setup() -> Runner {
        let buf_size = 10;
        let client_addr: SocketAddr = Gen::socket_addr();
        let peer_addresses: Vec<SocketAddr> =
            vec![Gen::socket_addr(), Gen::socket_addr(), Gen::socket_addr()];

        let (conn_tx, conn_rx) = mpsc::channel::<SocketAddr>(buf_size);
        let (req_tx, req_rx) = mpsc::channel::<(SocketAddr, Request)>(buf_size);

        for peer_addr in peer_addresses.clone().into_iter() {
            let listener = TcpListener::bind(peer_addr).await.unwrap();
            let conn_tx = conn_tx.clone();
            let msg_tx = req_tx.clone();

            tokio::spawn(async move {
                loop {
                    let (socket, client_addr) = listener.accept().await.unwrap();
                    // println!("> Peer listening at {:?}", peer_addr);

                    conn_tx.send(client_addr.clone()).await.unwrap();
                    let msg_tx = msg_tx.clone();

                    tokio::spawn(async move {
                        let conn = ServerConnection::new(socket);
                        loop {
                            let read_msg = conn.read().await.unwrap();
                            // println!("> Peer at {:?} got request: {:?}", peer_addr, msg);
                            msg_tx.send((peer_addr, read_msg)).await.unwrap();
                        }
                    });
                }
            });
        }

        return Runner {
            client_addr,
            server_addrs: peer_addresses,
            conn_rx,
            req_rx,
        };
    }

    #[tokio::test]
    async fn constructs_itself() {
        let client_addr = Gen::socket_addr();
        let server_addrs = vec![Gen::socket_addr(), Gen::socket_addr(), Gen::socket_addr()];
        let client = Client::new(client_addr, server_addrs.clone());

        assert_eq!(client.address, client_addr);
        assert_eq!(client.peer_addresses, server_addrs.clone());
        assert!(client.peers.is_empty());
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
        assert_eq!(client.peers.len(), server_addrs.len())
    }

    #[tokio::test]
    async fn writes_to_a_peer() {
        let Runner {
            client_addr,
            server_addrs,
            mut conn_rx,
            mut req_rx,
            ..
        } = setup().await;

        let mut client = Client::new(client_addr, server_addrs.clone());
        client.run().await.unwrap();
        for _ in 0..2 {
            let _ = conn_rx.recv().await.unwrap();
        }

        let _ = client.write_one(&server_addrs[0].to_string(), &*REQ).await;
        let (conn, received_msg) = req_rx.recv().await.unwrap();

        assert_eq!(conn, server_addrs[0]);
        assert_eq!(received_msg, *REQ);
    }

    #[tokio::test]
    async fn broadcasts_to_all_peers() {
        let Runner {
            client_addr,
            server_addrs,
            mut conn_rx,
            mut req_rx,
            ..
        } = setup().await;

        let mut client = Client::new(client_addr, server_addrs.clone());
        client.run().await.unwrap();
        for _ in 0..2 {
            let _ = conn_rx.recv().await.unwrap();
        }

        let _ = client.broadcast(&*REQ).await;

        let (peer_1, msg_1) = req_rx.recv().await.unwrap();
        let (peer_2, msg_2) = req_rx.recv().await.unwrap();
        let (peer_3, msg_3) = req_rx.recv().await.unwrap();

        assert_eq!(
            vec![msg_1, msg_2, msg_3],
            vec![REQ.clone(), REQ.clone(), REQ.clone()]
        );
        assert_eq!(
            HashSet::<_>::from_iter(vec![peer_1, peer_2, peer_3].into_iter()),
            HashSet::<_>::from_iter(server_addrs.into_iter()),
        );
    }

    #[tokio::test]
    async fn writes_to_many_peers() {
        let Runner {
            client_addr,
            server_addrs,
            mut conn_rx,
            mut req_rx,
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

        let _ = client.write_many(&recipient_addrs, &*REQ).await;
        let (peer_1, msg_1) = req_rx.recv().await.unwrap();
        let (peer_2, msg_2) = req_rx.recv().await.unwrap();

        assert_eq!(vec![msg_1, msg_2], vec![REQ.clone(), REQ.clone()],);
        assert_eq!(
            HashSet::<_>::from_iter(vec![peer_1.to_string(), peer_2.to_string()].into_iter()),
            HashSet::<_>::from_iter(recipient_addrs.into_iter()),
        );
    }
}
