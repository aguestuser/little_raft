use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use dashmap::DashMap;
use futures::future;
use futures::stream;
use futures::StreamExt;
use tokio::net::TcpSocket;
use tokio::sync::oneshot;
use tokio::sync::oneshot::Sender as OneShotSender;
use tokio::time;
use tokio::time::Duration;

use crate::error::NetworkError::{
    BroadcastFailure, NoPeerAtAddress, PeerConnectionClosed, RequestTimeout, TaskJoinFailure,
};
use crate::error::{AsyncError, Result};
use crate::protocol::connection::ClientConnection;
use crate::protocol::request::{Command, Request};
use crate::protocol::response::Response;

#[cfg(not(test))]
const BROADCAST_TIMEOUT_MILLIS: u64 = 1000;
#[cfg(test)]
const BROADCAST_TIMEOUT_MILLIS: u64 = 10;
#[cfg(not(test))]
const DM_TIMEOUT_MILLIS: u64 = 1000;
#[cfg(test)]
const DM_TIMEOUT_MILLIS: u64 = 20;

pub struct Client {
    peer_addresses: Vec<SocketAddr>,
    peers: DashMap<String, Peer>,
    response_handlers: Arc<DashMap<u64, OneShotSender<Response>>>,
    request_id: AtomicU64,
}

#[derive(Clone)]
pub struct ClientConfig {
    pub peer_addresses: Vec<SocketAddr>,
}

pub struct Peer {
    address: SocketAddr, // TODO: should this be a String?
    connection: Arc<ClientConnection>,
}

impl Client {
    /// Construct a `Client` from a `Client` config, leaving "live" resources to be initialized
    /// later in `Client::run`.
    pub fn new(cfg: ClientConfig) -> Client {
        let ClientConfig { peer_addresses } = cfg;
        Self {
            peer_addresses,
            peers: DashMap::new(),
            response_handlers: Arc::new(DashMap::new()),
            request_id: AtomicU64::new(0),
        }
    }

    /// Atomically fetch and increment an id for request tagging (this enables us to tell
    /// which responses correspond to which requests while enabling the same underlying
    /// command to be issued to multiple peers, each with a different id).
    pub fn next_id(&self) -> u64 {
        self.request_id.fetch_add(1, Ordering::SeqCst)
    }

    /// Create TCP socket connections to all peers, then store a reference to each connection, and
    /// listen for responses on it, forwarding any responses to one-shot-receiver handlers registered
    /// in `Client::write`  and removing the handlers from the handler registry once they are used.
    pub async fn run(&self) -> Result<()> {
        // connect to each peer in parallel, returning an Err if any connection fails
        let peers: Vec<Peer> = future::try_join_all(
            self.peer_addresses
                .iter()
                .map(|&address| async move {
                    let stream = TcpSocket::new_v4()?.connect(address).await?;
                    let connection = Arc::new(ClientConnection::new(stream));
                    Ok(Peer {
                        address,
                        connection,
                    })
                })
                .map(tokio::spawn),
        )
        .await?
        .into_iter()
        .collect::<Result<Vec<Peer>>>()?;

        // store connections to peers keyed by their address and handle responses on each connection
        let par_factor = peers.len();
        let _ = stream::iter(peers)
            .for_each_concurrent(par_factor, |peer| async {
                // clone values we need to move
                let connection = peer.connection.clone();
                let handlers = self.response_handlers.clone();
                // store reference to peer in hashmap
                self.peers.insert(peer.address.to_string(), peer);
                // listen for responses from each peer in a loop in a new task
                tokio::spawn(async move {
                    loop {
                        if let Ok(response) = connection.read().await {
                            // send the responses over a oneshot channel to handlers registered in #write (below)
                            if let Some((_, handler)) = handlers.remove(&response.id()) {
                                let _ = handler.send(response);
                            }
                        }
                    }
                });
            })
            .await;

        Ok(())
    }

    /// Write a `request` to a peer `connection` and register a one-shot sender to
    /// handle the peer's response in the shared `response_handlers` hash map owned by the `Client`.
    /// Then wait to either receive the response and return an `Ok<Response>` or timeout
    /// and return an `Err`.
    async fn write(
        connection: Arc<ClientConnection>,
        response_handlers: Arc<DashMap<u64, OneShotSender<Response>>>,
        request: Request,
        timeout_in_millis: u64,
    ) -> Result<Response> {
        let (response_tx, response_rx) = oneshot::channel::<Response>();
        let _ = response_handlers.insert(request.id, response_tx);
        connection.write(request).await?;

        return tokio::select! {
            response = response_rx => {
                response.map_err(|_| Box::new(PeerConnectionClosed) as AsyncError)
            }
            _ = time::sleep(Duration::from_millis(timeout_in_millis)) => {
                Err(Box::new(RequestTimeout))
            }
        };
    }

    /// Send a `command` to a single peer at a given `peer_address`, returning an `Err` if the
    /// address is not registered with this client or if transmission fails, otherwise return
    /// the `Ok<Response>` returned by `Client::write`
    pub async fn write_one(&self, peer_address: &String, command: &Command) -> Result<Response> {
        if let Some(peer) = self.peers.get(peer_address) {
            Client::write(
                peer.connection.clone(),
                self.response_handlers.clone(),
                Request {
                    id: self.next_id(),
                    command: command.clone(),
                },
                DM_TIMEOUT_MILLIS,
            )
            .await
        } else {
            Err(Box::new(NoPeerAtAddress(peer_address.to_string())))
        }
    }

    /// Broadcast a `command` to all peers in parallel, then wait for a majority of peers to reply
    /// with any response by delegating to `Client::broadcast_and_filter` with an always-true filter.
    pub async fn broadcast(&self, command: Command) -> Result<Vec<Response>> {
        self.broadcast_and_filter(command, |r| Some(r)).await
    }

    /// Broadcast a `command` to all peers in parallel, then wait for a majority of peers to reply with
    /// a response that satisfies some `filter` predicate. If a majority of peers either fail to
    /// respond or respond in a manner that fails to satisfy the predicate, return an `Err` indicating
    /// the broadcast has failed. Otherwise, return a `Vec` of the successful responses as soon as they
    /// arrive from a majority of peers, without waiting for further responses from other peers.
    pub async fn broadcast_and_filter(
        &self,
        command: Command,
        filter: impl Fn(Response) -> Option<Response>,
    ) -> Result<Vec<Response>> {
        let num_peers = self.peers.len();
        let majority = num_peers / 2;

        let connections: Vec<Arc<ClientConnection>> = self
            .peers
            .iter()
            .map(|e| e.value().connection.clone())
            .collect();

        let successful_responses = stream::iter(connections)
            .map(|connection| {
                let handlers = self.response_handlers.clone();
                let id = self.next_id();
                let command = command.clone();
                Client::write(
                    connection.clone(),
                    handlers,
                    Request { id, command },
                    BROADCAST_TIMEOUT_MILLIS,
                )
            })
            .map(tokio::spawn)
            .buffer_unordered(num_peers)
            .filter_map(|join_handle| async {
                join_handle
                    .unwrap_or(Err(Box::new(TaskJoinFailure) as AsyncError))
                    .map_or(None, |response| filter(response))
            })
            .take(majority)
            .collect::<Vec<Response>>()
            .await;

        if successful_responses.len() < majority {
            // error if a majority of peers either don't respond (timeout) or respond unsuccessfully
            Err(Box::new(BroadcastFailure) as AsyncError)
        } else {
            Ok(successful_responses)
        }
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
    use tokio::sync::mpsc::Receiver;
    use tokio::sync::{mpsc, Mutex};

    use crate::protocol::connection::ServerConnection;
    use crate::protocol::request::Command;
    use crate::protocol::response::Outcome;
    use crate::test_support::gen::Gen;

    use super::*;
    use crate::error::NetworkError;

    struct Runner {
        client_config: ClientConfig,
        peer_addresses: Vec<SocketAddr>,
        recipient_addresses: Vec<String>,
        req_rx: Receiver<(SocketAddr, Request)>,
    }

    lazy_static! {
        static ref NUM_PEERS: usize = 5;
        static ref MAJORITY: usize = *NUM_PEERS / 2;
        static ref GET_CMD: Command = Command::Get {
            key: "foo".to_string()
        };
        static ref SET_CMD: Command = Command::Set {
            key: "foo".to_string(),
            value: "bar".to_string(),
        };
        static ref INVALID_CMD: Command = Command::Invalid {
            msg: "foo".to_string(),
        };
    }

    async fn setup() -> Runner {
        setup_with(Vec::new(), Vec::new()).await
    }

    async fn setup_with_outcomes(outcomes: Vec<Outcome>) -> Runner {
        setup_with(outcomes, Vec::new()).await
    }

    async fn setup_with(outcomes: Vec<Outcome>, fuzzed_ids: Vec<u64>) -> Runner {
        let buf_size = *NUM_PEERS;
        let outcomes = Arc::new(outcomes);
        let fuzzed_ids = Arc::new(fuzzed_ids);

        let peer_addresses: Vec<SocketAddr> = (0..*NUM_PEERS).map(|_| Gen::socket_addr()).collect();
        let (req_tx, req_rx) = mpsc::channel::<(SocketAddr, Request)>(buf_size);

        for (peer_idx, peer_addr) in peer_addresses.clone().into_iter().enumerate() {
            let listener = TcpListener::bind(peer_addr).await.unwrap();
            let req_tx = req_tx.clone();
            let outcomes = outcomes.clone();
            let ids = fuzzed_ids.clone();

            tokio::spawn(async move {
                for _ in 0..*NUM_PEERS {
                    let (socket, _) = listener.accept().await.unwrap();
                    // println!("> Peer listening at {:?}", peer_addr);

                    let req_tx = req_tx.clone();
                    let outcomes = outcomes.clone();
                    let fuzzed_ids = ids.clone();

                    tokio::spawn(async move {
                        let conn = ServerConnection::new(socket);
                        loop {
                            let req = conn.read().await.unwrap();
                            // println!("> Peer at {:?} got request: {:?}", peer_addr, req);
                            // report receipt of request to test harness receiver
                            req_tx.send((peer_addr, req.clone())).await.unwrap();
                            // send canned response provided by test harness to client
                            if !outcomes.is_empty() {
                                let response = Response {
                                    // respond with request id unless we have provided fuzzed ids
                                    id: if fuzzed_ids.is_empty() {
                                        req.id
                                    } else {
                                        fuzzed_ids[peer_idx]
                                    },
                                    outcome: outcomes[peer_idx].clone(),
                                };
                                conn.write(response).await.unwrap();
                            }
                        }
                    });
                }
            });
        }

        return Runner {
            peer_addresses: peer_addresses.clone(),
            recipient_addresses: peer_addresses
                .clone()
                .iter()
                .map(|sa| sa.to_string())
                .collect(),
            client_config: ClientConfig { peer_addresses },
            req_rx,
        };
    }

    #[tokio::test]
    async fn constructs_a_client() {
        let cfg = ClientConfig {
            peer_addresses: vec![Gen::socket_addr(), Gen::socket_addr(), Gen::socket_addr()],
        };
        let client = Client::new(cfg.clone());

        assert_eq!(client.peer_addresses, cfg.peer_addresses.clone());
        assert!(client.peers.is_empty());
    }

    #[tokio::test]
    async fn provides_incrementing_ids() {
        let client = Client::new(Gen::client_config());

        assert_eq!(client.next_id(), 0);
        assert_eq!(client.next_id(), 1);
    }

    #[tokio::test]
    async fn connects_to_peers() {
        let Runner {
            recipient_addresses,
            client_config,
            ..
        } = setup().await;

        let client = Client::new(client_config);
        client.run().await.unwrap();

        assert_eq!(client.peers.len(), *NUM_PEERS);
        assert_eq!(
            client
                .peers
                .into_read_only()
                .keys()
                .collect::<HashSet<&String>>(),
            HashSet::from_iter(recipient_addresses.iter())
        );
    }

    #[tokio::test]
    async fn writes_to_a_peer() {
        let Runner {
            peer_addresses,
            recipient_addresses,
            client_config,
            mut req_rx,
            ..
        } = setup().await;

        let client = Client::new(client_config);
        client.run().await.unwrap();

        let _ = client.write_one(&recipient_addresses[0], &*GET_CMD).await;
        let (conn, received_msg) = req_rx.recv().await.unwrap();

        assert_eq!(conn, peer_addresses[0]);
        assert_eq!(received_msg.command, *GET_CMD);
    }

    #[tokio::test]
    async fn handles_response_from_a_peer() {
        let outcomes = vec![Gen::outcome_of(GET_CMD.clone())];
        let Runner {
            peer_addresses,
            client_config,
            req_rx,
            ..
        } = setup_with_outcomes(outcomes.clone()).await;

        let client = Client::new(client_config);
        client.run().await.unwrap();
        let _ = req_rx; // if we don't reference req_rx in the test scope requests are not received

        let resp = client
            .write_one(&peer_addresses[0].to_string(), &*GET_CMD)
            .await;

        assert_eq!(resp.unwrap().outcome, outcomes[0]);
    }

    #[tokio::test]
    async fn handles_timeout_from_a_peer() {
        let Runner {
            peer_addresses,
            client_config,
            mut req_rx,
            ..
        } = setup_with(vec![Gen::outcome()], vec![Gen::u64()]).await;

        let client = Client::new(client_config);
        client.run().await.unwrap();

        let resp = client
            .write_one(&peer_addresses[0].to_string(), &*GET_CMD)
            .await;
        let (_, _) = req_rx.recv().await.unwrap();

        assert!(resp.is_err());
        assert_eq!(resp.err().unwrap().to_string(), RequestTimeout.to_string());
    }

    #[tokio::test]
    async fn broadcasts_request_to_all_peers() {
        let Runner {
            peer_addresses,
            client_config,
            req_rx,
            ..
        } = setup().await;

        let a_req_rx = Arc::new(Mutex::new(req_rx));
        let client = Client::new(client_config);
        let _ = client.run().await.unwrap();
        let _ = client.broadcast(GET_CMD.clone()).await;
        let _ = req_rx;

        let (expected_receiving_peers, expected_received_requests) = (
            HashSet::from_iter(peer_addresses.into_iter()),
            HashSet::from_iter((0..5).map(|id| Request {
                id,
                command: GET_CMD.clone(),
            })),
        );

        let (actual_receiving_peers, actual_received_requests): (
            HashSet<SocketAddr>,
            HashSet<Request>,
        ) = futures::stream::iter(0..5)
            .map(|_| {
                let rx = a_req_rx.clone();
                async move { rx.lock().await.recv().await.unwrap() }
            })
            .buffer_unordered(5)
            .collect::<HashSet<(SocketAddr, Request)>>()
            .await
            .into_iter()
            .unzip();

        assert_eq!(actual_receiving_peers, expected_receiving_peers);
        assert_eq!(actual_received_requests, expected_received_requests,);
    }

    #[tokio::test]
    async fn handles_broadcast_response_from_majority_of_peers() {
        let mocked_outcomes = std::iter::repeat(Gen::outcome_of(GET_CMD.clone()))
            .take(*NUM_PEERS)
            .collect::<Vec<Outcome>>();

        let Runner {
            client_config,
            req_rx,
            ..
        } = setup_with_outcomes(mocked_outcomes.clone()).await;

        let client = Client::new(client_config);
        let _ = client.run().await.unwrap();
        let _ = req_rx;

        let outcomes: Vec<Outcome> = client
            .broadcast(GET_CMD.clone())
            .await
            .unwrap()
            .into_iter()
            .map(|response| response.outcome)
            .collect();

        assert_eq!(outcomes, mocked_outcomes.clone()[0..*MAJORITY],);
    }

    #[tokio::test]
    async fn handles_broadcast_timeout() {
        let Runner {
            client_config,
            req_rx,
            ..
        } = setup().await;

        let client = Client::new(client_config);
        let _ = client.run().await.unwrap();
        let _ = req_rx;

        let result = client.broadcast(GET_CMD.clone()).await;
        assert_eq!(
            result.err().unwrap().to_string(),
            NetworkError::BroadcastFailure.to_string(),
        );
    }

    #[tokio::test]
    async fn filters_broadcast_responses_by_predicate() {
        let get_outcome = Gen::outcome_of(GET_CMD.clone());
        let err_outcome = Outcome::Error {
            msg: "foo".to_string(),
        };
        let mocked_outcomes = (0..*NUM_PEERS)
            .map(|n| {
                if n > 0 {
                    err_outcome.clone()
                } else {
                    get_outcome.clone()
                }
            })
            .collect::<Vec<_>>();

        let Runner {
            client_config,
            req_rx,
            ..
        } = setup_with_outcomes(mocked_outcomes.clone()).await;

        let client = Client::new(client_config);
        let _ = client.run().await.unwrap();
        let _ = req_rx;
        let filter = |resp: Response| match resp.outcome {
            Outcome::OfGet { .. } => Some(resp),
            _ => None,
        };

        let result = client.broadcast_and_filter(GET_CMD.clone(), filter).await;

        assert_eq!(
            result.err().unwrap().to_string(),
            NetworkError::BroadcastFailure.to_string(),
        );
    }
}
