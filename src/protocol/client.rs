use std::net::SocketAddr;
use std::sync::Arc;

use dashmap::DashMap;
use futures::StreamExt;
use tokio::net::TcpSocket;

use crate::error::NetworkError::{PeerConnectionClosed, RequestTimeout};
use crate::error::{AsyncError, NetworkError, Result};
use crate::node::State;
use crate::protocol::connection::ClientConnection;
use crate::protocol::request::{Command, Request};
use crate::protocol::response::Response;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::mpsc;
use tokio::sync::mpsc::Sender;
use tokio::time;
use tokio::time::Duration;

// TODO: parameterize this as an arg to `Client::new` (maybe on a config struct?)
const RESPONSE_TIMEOUT_IN_MILLIS: u64 = 1000;
const RESPONSE_BUFFER_SIZE: usize = 16;

pub struct Client {
    pub address: SocketAddr,
    pub peer_addresses: Vec<SocketAddr>,
    pub state: State,
    pub peers: DashMap<String, Peer>,
    pub response_handlers: Arc<DashMap<u64, Sender<Response>>>,
    pub request_id: AtomicU64,
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
            response_handlers: Arc::new(DashMap::new()),
            request_id: AtomicU64::new(0),
        }
    }

    pub fn next_id(&self) -> u64 {
        self.request_id.fetch_add(1, Ordering::SeqCst)
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

        let (response_tx, mut response_rx) = mpsc::channel::<Response>(RESPONSE_BUFFER_SIZE);

        // register peers and listen for responses on their connections
        peers.into_iter().for_each(|peer_result| {
            if let Ok(peer) = peer_result {
                // listen for responses in a loop on each connection
                let connection = peer.connection.clone();
                let response_tx = response_tx.clone();
                tokio::spawn(async move {
                    loop {
                        if let Ok(response) = connection.read().await {
                            let _ = response_tx.send(response).await;
                        }
                    }
                });

                self.peers.insert(peer.address.to_string(), peer);
            }
        });

        // forward responses to registered handlers
        let handlers = self.response_handlers.clone();
        tokio::spawn(async move {
            loop {
                if let Some(response) = response_rx.recv().await {
                    if let Some(handler) = handlers.get(&response.id()).as_deref() {
                        let _ = handler.send(response.clone()).await;
                    }
                }
            }
        });

        Ok(())
    }

    async fn write(
        connection: Arc<ClientConnection>,
        response_handlers: Arc<DashMap<u64, Sender<Response>>>,
        req: Request,
    ) -> Result<Response> {
        // TODO: use oneshot channels here!!
        let (tx, mut rx) = mpsc::channel::<Response>(1);
        let _ = response_handlers.insert(req.id, tx);

        connection.write(req.clone()).await?;

        let resp: Result<Response> = tokio::select! {
            r = rx.recv() => {
                // TODO: eliminate this unwrapping and error-handling once oneshot used!
                r.ok_or_else(|| Box::new(PeerConnectionClosed) as AsyncError)
            }
            _ = time::sleep(Duration::from_millis(RESPONSE_TIMEOUT_IN_MILLIS)) => {
                Err(Box::new(RequestTimeout))
            }
        };

        resp
    }

    pub async fn write_one(&mut self, peer_addr: &String, cmd: &Command) -> Result<Response> {
        if let Some(peer) = self.peers.get(peer_addr) {
            Self::write(
                peer.connection.clone(),
                self.response_handlers.clone(),
                Request {
                    id: self.next_id(),
                    command: cmd.clone(),
                },
            )
            .await
        } else {
            Err(Box::new(NetworkError::NoPeerAtAddress(
                peer_addr.to_string(),
            )))
        }
    }

    pub async fn write_many(
        &mut self,
        peer_addrs: &Vec<String>,
        cmd: &Command,
    ) -> Vec<Result<Response>> {
        let num_writes = peer_addrs.len();

        let peers_by_address = peer_addrs
            .clone()
            .into_iter()
            .map(|peer_addr| (self.peers.get(&peer_addr), peer_addr));

        let writes =
            futures::stream::iter(peers_by_address.map(|(peer_entry, addr)| match peer_entry {
                Some(peer) => {
                    let handlers = self.response_handlers.clone();
                    tokio::spawn(Self::write(
                        peer.connection.clone(),
                        handlers,
                        Request {
                            id: self.next_id(),
                            command: cmd.clone(),
                        },
                    ))
                }
                None => tokio::spawn(async move {
                    // TODO: avoid spawning here since no work to do (blocker: matching `Error` types!)
                    Err(Box::new(NetworkError::NoPeerAtAddress(addr.to_string())) as AsyncError)
                }),
            }));

        writes
            .buffer_unordered(num_writes)
            .map(|r| r.unwrap_or_else(|e| Err(e.into()))) // un-nest Result<Result>
            .collect::<Vec<Result<Response>>>()
            .await
    }

    pub async fn broadcast(&mut self, cmd: &Command) -> Vec<Result<Response>> {
        let peer_addrs = self
            .peers
            .iter()
            .map(|entry| entry.key().to_string())
            .collect::<Vec<String>>();
        self.write_many(&peer_addrs, cmd).await
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
    use crate::protocol::request::Command;
    use crate::test_support::gen::Gen;

    use super::*;
    use crate::protocol::response::Outcome;

    struct Runner {
        client_addr: SocketAddr,
        peer_addrs: Vec<SocketAddr>,
        recipient_addrs: Vec<String>,
        conn_rx: Receiver<SocketAddr>,
        req_rx: Receiver<(SocketAddr, Request)>,
    }

    lazy_static! {
        static ref CMD: Command = Command::Get {
            key: "foo".to_string()
        };
        static ref RESP: Response = Response {
            id: 42,
            outcome: Outcome::OfGet {
                value: Some("foo".to_string()),
            }
        };
        static ref OUTCOME: Outcome = Outcome::OfGet {
            value: Some("foo".to_string()),
        };
    }

    async fn setup_no_responses() -> Runner {
        setup(Vec::new(), Vec::new()).await
    }

    async fn setup_no_ids(outcomes: Vec<Outcome>) -> Runner {
        setup(outcomes, Vec::new()).await
    }

    async fn setup(outcomes: Vec<Outcome>, ids: Vec<u64>) -> Runner {
        let buf_size = 10;
        let outcomes = Arc::new(outcomes);
        let ids = Arc::new(ids);

        let client_addr: SocketAddr = Gen::socket_addr();
        let peer_addresses: Vec<SocketAddr> =
            vec![Gen::socket_addr(), Gen::socket_addr(), Gen::socket_addr()];

        let (conn_tx, conn_rx) = mpsc::channel::<SocketAddr>(buf_size);
        let (req_tx, req_rx) = mpsc::channel::<(SocketAddr, Request)>(buf_size);

        for peer_addr in peer_addresses.clone().into_iter() {
            let listener = TcpListener::bind(peer_addr).await.unwrap();
            let conn_tx = conn_tx.clone();
            let msg_tx = req_tx.clone();
            let outcomes = outcomes.clone();
            let ids = ids.clone();

            tokio::spawn(async move {
                loop {
                    let (socket, client_addr) = listener.accept().await.unwrap();
                    // println!("> Peer listening at {:?}", peer_addr);

                    conn_tx.send(client_addr.clone()).await.unwrap();
                    let msg_tx = msg_tx.clone();
                    let outcomes = outcomes.clone();
                    let ids = ids.clone();

                    tokio::spawn(async move {
                        let conn = ServerConnection::new(socket);
                        let i = 0;
                        loop {
                            let req = conn.read().await.unwrap();
                            // report receipt of request to test harness
                            // println!("> Peer at {:?} got request: {:?}", peer_addr, read_msg);
                            msg_tx.send((peer_addr, req.clone())).await.unwrap();
                            // send canned response provided by test harness to client
                            if let Some(outcome) = outcomes.get(i) {
                                let response = Response {
                                    id: if ids.is_empty() { req.id } else { ids[i] },
                                    outcome: outcome.clone(),
                                };
                                conn.write(response).await.unwrap();
                            }
                        }
                    });
                }
            });
        }

        return Runner {
            client_addr,
            peer_addrs: peer_addresses.clone(),
            recipient_addrs: peer_addresses.iter().map(|sa| sa.to_string()).collect(),
            conn_rx,
            req_rx,
        };
    }

    #[tokio::test]
    async fn constructs_a_client() {
        let client_addr = Gen::socket_addr();
        let server_addrs = vec![Gen::socket_addr(), Gen::socket_addr(), Gen::socket_addr()];
        let client = Client::new(client_addr, server_addrs.clone());

        assert_eq!(client.address, client_addr);
        assert_eq!(client.peer_addresses, server_addrs.clone());
        assert!(client.peers.is_empty());
    }

    #[tokio::test]
    async fn provides_incrementing_ids() {
        let client = Client::new(Gen::socket_addr(), vec![Gen::socket_addr()]);

        assert_eq!(client.next_id(), 0);
        assert_eq!(client.next_id(), 1);
    }

    #[tokio::test]
    async fn connects_to_servers() {
        let Runner {
            client_addr,
            peer_addrs: server_addrs,
            mut conn_rx,
            ..
        } = setup_no_responses().await;
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
            peer_addrs,
            recipient_addrs,
            mut conn_rx,
            mut req_rx,
            ..
        } = setup_no_responses().await;

        let mut client = Client::new(client_addr, peer_addrs.clone());
        client.run().await.unwrap();
        for _ in 0..2 {
            let _ = conn_rx.recv().await.unwrap();
        }

        let _ = client.write_one(&recipient_addrs[0], &*CMD).await;
        let (conn, received_msg) = req_rx.recv().await.unwrap();

        assert_eq!(conn, peer_addrs[0]);
        assert_eq!(received_msg.command, *CMD);
    }

    #[tokio::test]
    async fn writes_to_many_peers() {
        let Runner {
            client_addr,
            peer_addrs,
            recipient_addrs,
            mut conn_rx,
            mut req_rx,
            ..
        } = setup_no_responses().await;

        let mut client = Client::new(client_addr, peer_addrs.clone());
        client.run().await.unwrap();
        for _ in 0..2 {
            let _ = conn_rx.recv().await.unwrap();
        }

        let recipient_addrs = recipient_addrs[0..2].to_vec();
        let _ = client.write_many(&recipient_addrs, &*CMD).await;
        let (peer_1, msg_1) = req_rx.recv().await.unwrap();
        let (peer_2, msg_2) = req_rx.recv().await.unwrap();

        assert_eq!(
            vec![msg_1.command, msg_2.command],
            vec![CMD.clone(), CMD.clone()],
        );
        assert_eq!(
            HashSet::<_>::from_iter(vec![peer_1.to_string(), peer_2.to_string()].into_iter()),
            HashSet::<_>::from_iter(recipient_addrs.into_iter()),
        );
    }

    #[tokio::test]
    async fn writes_to_all_peers() {
        let Runner {
            client_addr,
            peer_addrs,
            mut conn_rx,
            mut req_rx,
            ..
        } = setup_no_responses().await;

        let mut client = Client::new(client_addr, peer_addrs.clone());
        client.run().await.unwrap();
        for _ in 0..2 {
            let _ = conn_rx.recv().await.unwrap();
        }

        let _ = client.broadcast(&*CMD).await;

        let (peer_1, req_1) = req_rx.recv().await.unwrap();
        let (peer_2, req_2) = req_rx.recv().await.unwrap();
        let (peer_3, req_3) = req_rx.recv().await.unwrap();

        assert_eq!(
            vec![req_1.command, req_2.command, req_3.command],
            vec![CMD.clone(), CMD.clone(), CMD.clone()]
        );
        assert_eq!(
            HashSet::<_>::from_iter(vec![peer_1, peer_2, peer_3].into_iter()),
            HashSet::<_>::from_iter(peer_addrs.into_iter()),
        );
    }

    #[tokio::test]
    async fn handles_response_from_a_peer() {
        let outcomes = vec![Gen::outcome_of(CMD.clone())];
        let Runner {
            client_addr,
            peer_addrs,
            mut conn_rx,
            mut req_rx,
            ..
        } = setup_no_ids(outcomes.clone()).await;

        let mut client = Client::new(client_addr, peer_addrs.clone());
        client.run().await.unwrap();
        for _ in 0..2 {
            let _ = conn_rx.recv().await.unwrap();
        }

        let resp = client.write_one(&peer_addrs[0].to_string(), &*CMD).await;
        let (_, _) = req_rx.recv().await.unwrap();

        assert_eq!(resp.unwrap().outcome, outcomes[0]);
    }

    #[tokio::test]
    async fn handles_timeout_from_a_peer() {
        let Runner {
            client_addr,
            peer_addrs,
            mut conn_rx,
            mut req_rx,
            ..
        } = setup(vec![Gen::outcome()], vec![Gen::u64()]).await;

        let mut client = Client::new(client_addr, peer_addrs.clone());
        client.run().await.unwrap();
        for _ in 0..2 {
            let _ = conn_rx.recv().await.unwrap();
        }

        let resp = client.write_one(&peer_addrs[0].to_string(), &*CMD).await;
        let (_, _) = req_rx.recv().await.unwrap();

        assert!(resp.is_err());
        assert_eq!(resp.err().unwrap().to_string(), RequestTimeout.to_string());
    }

    #[tokio::test]
    async fn handles_response_from_many_peers() {
        let expected_outcome = Gen::outcome_of(CMD.clone());
        let Runner {
            client_addr,
            peer_addrs,
            recipient_addrs,
            mut conn_rx,
            mut req_rx,
            ..
        } = setup_no_ids(vec![expected_outcome.clone()]).await;

        let mut client = Client::new(client_addr, peer_addrs.clone());
        client.run().await.unwrap();
        for _ in 0..2 {
            let _ = conn_rx.recv().await.unwrap();
        }

        let results = client.write_many(&recipient_addrs, &*CMD).await;
        let (_, _) = req_rx.recv().await.unwrap();
        let (_, _) = req_rx.recv().await.unwrap();
        let (_, _) = req_rx.recv().await.unwrap();

        let actual_outcomes = results
            .into_iter()
            .map(|r| r.unwrap().outcome)
            .collect::<Vec<Outcome>>();

        assert_eq!(actual_outcomes.len(), recipient_addrs.len());
        actual_outcomes.iter().for_each(|actual_outcome| {
            assert_eq!(actual_outcome.clone(), expected_outcome);
        });
    }

    #[tokio::test]
    async fn handles_timeouts_from_many_peers() {
        let Runner {
            client_addr,
            peer_addrs,
            mut conn_rx,
            mut req_rx,
            ..
        } = setup(vec![Gen::outcome()], vec![Gen::u64()]).await;

        let mut client = Client::new(client_addr, peer_addrs.clone());
        client.run().await.unwrap();
        for _ in 0..2 {
            let _ = conn_rx.recv().await.unwrap();
        }

        let recipient_addrs = peer_addrs[0..2]
            .iter()
            .map(|x| x.to_string())
            .collect::<Vec<String>>();

        let results = client.write_many(&recipient_addrs, &*CMD).await;
        let (_, _) = req_rx.recv().await.unwrap();
        let (_, _) = req_rx.recv().await.unwrap();

        results.iter().for_each(|r| {
            assert!(r.is_err());
            assert_eq!(
                r.as_ref().err().unwrap().to_string(),
                RequestTimeout.to_string()
            );
        });
    }
}
