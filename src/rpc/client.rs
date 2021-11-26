use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use dashmap::DashMap;
use futures::future;
use futures::stream;
use futures::StreamExt;
use tokio::net::TcpStream;

use crate::error::NetworkError::{BroadcastFailure, ConnectionClosed, NoPeerAtAddress};
use crate::error::Result;
use crate::rpc::request::{RpcRequest, RpcRequestEnvelope};
use crate::rpc::response::{RpcResponse, RpcResponseEnvelope};
use crate::rpc::RpcClientConnection;

use crate::NodeAddr;

use tokio::sync::mpsc::Sender;

pub type RpcResponseInContext = (NodeAddr, RpcRequest, RpcResponse);

pub struct Peer {
    address: SocketAddr, // TODO: should this be a String?
    connection: Arc<RpcClientConnection>,
}

#[derive(Clone)]
pub struct RpcClientConfig {
    pub peer_addresses: Vec<SocketAddr>,
}

pub struct RpcClient {
    peers_by_address: Arc<DashMap<NodeAddr, Peer>>,
    request_id: AtomicU64,
    requests_by_id: Arc<DashMap<u64, RpcRequest>>,
}

impl RpcClientConfig {
    /// Create a live `RpcClient` from an inert `RpcClientConfig` as follows...
    /// Create TCP socket connections to all peers, then store a reference to each connection, and
    /// listen for responses on it, forwarding any responses to one-shot-receiver handlers registered
    /// in `Client::write`  and removing the handlers from the handler registry once they are used.
    pub async fn run_with(self, response_tx: Sender<RpcResponseInContext>) -> Result<RpcClient> {
        let peers_by_address = Arc::new(DashMap::new());
        let request_id = AtomicU64::new(0);
        let requests_by_id = Arc::new(DashMap::new());

        // connect to each peer in parallel, returning an Err if any connection fails
        let peers: Vec<Peer> = future::try_join_all(
            self.peer_addresses
                .iter()
                .map(|&address| async move {
                    let stream = TcpStream::connect(address).await?;
                    let connection = Arc::new(RpcClientConnection::new(stream));
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
        let num_peers = peers.len();
        let _ = stream::iter(peers)
            .for_each_concurrent(num_peers, |peer| async {
                // store reference to peer in hashmap (cloning values needed for response-handling before moving it)
                let connection = peer.connection.clone();
                let peer_address = peer.address.to_string();
                peers_by_address.insert(peer_address.clone(), peer);
                // listen for responses from each peer in a separate task
                let requests_by_id = requests_by_id.clone();
                let response_tx = response_tx.clone();
                tokio::spawn(async move {
                    loop {
                        match connection.read().await {
                            // on read, emit `ResponseInContext` tuple to `Node::handle_rpc_responses`
                            Ok(response_env) => {
                                let RpcResponseEnvelope { id, response } = response_env;
                                if let Some((_, request)) = requests_by_id.remove(&id) {
                                    let _ = response_tx
                                        .send((peer_address.clone(), request, response))
                                        .await;
                                }
                            }
                            // stop listening if client has closed connection
                            Err(e) => {
                                if e.downcast_ref() == Some(&ConnectionClosed) {
                                    return;
                                } else {
                                    eprintln!("{}", e);
                                }
                            }
                        }
                    }
                });
            })
            .await;

        Ok(RpcClient {
            peers_by_address,
            request_id,
            requests_by_id,
        })
    }
}

impl RpcClient {
    /// Atomically fetch and increment an id for request tagging (this enables us to tell
    /// which responses correspond to which requests while enabling the same underlying
    /// request to be issued to multiple peers, each with a different id).
    pub fn next_id(&self) -> u64 {
        self.request_id.fetch_add(1, Ordering::SeqCst)
    }

    /// TODO: docs!
    pub async fn send_many(&self, requests_by_peer: Vec<(NodeAddr, RpcRequest)>) -> Result<()> {
        let num_peers = requests_by_peer.len();

        let successful_responses = stream::iter(requests_by_peer)
            .map(|(peer_addr, request)| {
                let req_env = RpcRequestEnvelope {
                    id: self.next_id(),
                    request,
                };
                RpcClient::write(
                    req_env,
                    peer_addr,
                    self.requests_by_id.clone(),
                    self.peers_by_address.clone(),
                )
            })
            .map(tokio::spawn)
            .buffer_unordered(num_peers)
            .filter_map(|join_handle| async { join_handle.ok() })
            .take(num_peers)
            .collect::<Vec<Result<()>>>()
            .await;

        if successful_responses.len() < num_peers {
            // TODO: return which peers failed here for retry?
            Err(BroadcastFailure.boxed())
        } else {
            Ok(())
        }
    }

    /// TODO: docs!
    async fn write(
        request_env: RpcRequestEnvelope,
        peer_address: NodeAddr,
        requests_by_id: Arc<DashMap<u64, RpcRequest>>,
        peers_by_address: Arc<DashMap<NodeAddr, Peer>>,
    ) -> Result<()> {
        if let Some(peer) = peers_by_address.get(&peer_address) {
            let _ = requests_by_id.insert(request_env.id.clone(), request_env.request.clone());
            // TODO: timeout requests here (as in legacy rpc client)...
            peer.connection.write(request_env).await
        } else {
            Err(NoPeerAtAddress(peer_address).boxed())
        }
    }
}

/*********
 * TESTS *
 *********/

#[cfg(test)]
mod test_rpc_client {
    use std::collections::HashSet;
    use std::iter::FromIterator;
    use test_context::{test_context, AsyncTestContext};

    use tokio::net::TcpListener;
    use tokio::sync::mpsc;
    use tokio::sync::mpsc::Receiver;

    use crate::test_support::gen::Gen;

    use super::*;
    use crate::rpc::client::RpcResponseInContext;
    use crate::rpc::request::AppendEntriesRequest;
    use crate::rpc::response::AppendEntriesResponse;
    use crate::rpc::RpcServerConnection;
    use crate::CHAN_BUF_SIZE;

    lazy_static! {
        static ref NUM_PEERS: usize = 5;
        static ref MAJORITY: usize = *NUM_PEERS / 2;
        static ref APPEND_REQ: RpcRequest = RpcRequest::AppendEntries(AppendEntriesRequest {
            entries: Gen::log_entries(3),
            leader_address: Gen::socket_addr().to_string(),
            leader_commit: 0,
            leader_term: 0,
            prev_log_index: 0,
            prev_log_term: 0,
        });
        static ref APPEND_SUCCESS: RpcResponse =
            RpcResponse::ToAppendEntries(AppendEntriesResponse {
                peer_term: 0,
                success: true,
            });
        static ref APPEND_SUCCESSES_FROM_ALL_PEERS: Vec<RpcResponse> =
            std::iter::repeat(APPEND_SUCCESS.clone())
                .take(*NUM_PEERS)
                .collect();
    }

    struct Context {
        client: RpcClient,
        expected_responses: Vec<RpcResponse>,
        peer_addresses: Vec<SocketAddr>,
        recipient_addresses: Vec<String>,
        requests_by_peer: Vec<(NodeAddr, RpcRequest)>,
        request_rx: Receiver<(SocketAddr, RpcRequestEnvelope)>,
        response_rx: Receiver<RpcResponseInContext>,
    }

    impl Context {
        async fn setup(expected_responses: Vec<RpcResponse>, fuzz_ids: bool) -> Self {
            let buf_size = *NUM_PEERS;
            let responses = Arc::new(expected_responses.clone());

            let peer_addresses: Vec<SocketAddr> =
                (0..*NUM_PEERS).map(|_| Gen::socket_addr()).collect();
            let (request_tx, request_rx) =
                mpsc::channel::<(SocketAddr, RpcRequestEnvelope)>(buf_size);

            for (peer_idx, peer_addr) in peer_addresses.clone().into_iter().enumerate() {
                let listener = TcpListener::bind(peer_addr).await.unwrap();
                let request_tx = request_tx.clone();
                let responses = responses.clone();

                tokio::spawn(async move {
                    loop {
                        let (socket, _) = listener.accept().await.unwrap();
                        // println!("> Peer listening at {:?}", peer_addr);

                        let request_tx = request_tx.clone();
                        let responses = responses.clone();

                        tokio::spawn(async move {
                            let conn = RpcServerConnection::new(socket);
                            loop {
                                // for every request read from the connection, emit request to
                                // test harness receiver and send canned response provided by context
                                // (fuzzing ids to simulate unrelated traffic if testing timeouts)
                                let req = conn.read().await.unwrap();
                                // println!("> Peer at {:?} got request: {:?}", peer_addr, req);
                                request_tx.send((peer_addr, req.clone())).await.unwrap();
                                if let Some(response) = responses.get(peer_idx) {
                                    let response_env = RpcResponseEnvelope {
                                        id: if fuzz_ids { Gen::u64() } else { req.id },
                                        response: response.clone(),
                                    };
                                    conn.write(response_env).await.unwrap();
                                    conn.close().await.unwrap();
                                }
                            }
                        });
                    }
                });
            }

            let client_config = RpcClientConfig {
                peer_addresses: peer_addresses.clone(),
            };
            let (response_tx, response_rx) = mpsc::channel::<RpcResponseInContext>(CHAN_BUF_SIZE);
            let client = client_config.run_with(response_tx).await.unwrap();

            return Self {
                client,
                expected_responses,
                peer_addresses: peer_addresses.clone(),
                recipient_addresses: peer_addresses
                    .clone()
                    .iter()
                    .map(|sa| sa.to_string())
                    .collect(),
                requests_by_peer: peer_addresses
                    .iter()
                    .map(|pa| (pa.clone().to_string(), APPEND_REQ.clone()))
                    .collect(),
                request_rx,
                response_rx,
            };
        }
    }

    struct RunningClient(Context);

    #[async_trait::async_trait]
    impl AsyncTestContext for RunningClient {
        async fn setup() -> Self {
            let ctx = Context::setup(vec![], false).await;
            Self(ctx)
        }
    }

    struct ClientReceivingAppendSuccess(Context);

    #[async_trait::async_trait]
    impl AsyncTestContext for ClientReceivingAppendSuccess {
        async fn setup() -> Self {
            let ctx = Context::setup(APPEND_SUCCESSES_FROM_ALL_PEERS.clone(), false).await;
            Self(ctx)
        }
    }

    #[test_context(RunningClient)]
    #[tokio::test]
    async fn provides_incrementing_ids(ctx: RunningClient) {
        assert_eq!(ctx.0.client.next_id(), 0);
        assert_eq!(ctx.0.client.next_id(), 1);
    }

    #[test_context(RunningClient)]
    #[tokio::test]
    async fn connects_to_peers(ctx: RunningClient) {
        assert_eq!(ctx.0.client.peers_by_address.len(), *NUM_PEERS);
        assert_eq!(
            ctx.0
                .client
                .peers_by_address
                .iter()
                .map(|e| e.key().clone())
                .collect::<HashSet<String>>(),
            HashSet::from_iter(ctx.0.recipient_addresses.clone().into_iter())
        );
    }

    #[test_context(RunningClient)]
    #[tokio::test]
    async fn sends_requests_to_peers(mut ctx: RunningClient) {
        let _ = ctx
            .0
            .client
            .send_many(ctx.0.requests_by_peer.clone())
            .await
            .unwrap();

        let (expected_receiving_peers, expected_received_requests) = (
            HashSet::from_iter(ctx.0.peer_addresses.clone().into_iter()),
            HashSet::from_iter((0..5).map(|id| RpcRequestEnvelope {
                id,
                request: APPEND_REQ.clone(),
            })),
        );

        let mut actual_received_requests_by_peer = HashSet::new();
        for _ in 0..*NUM_PEERS {
            actual_received_requests_by_peer.insert(ctx.0.request_rx.recv().await.unwrap());
        }

        let (actual_receiving_peers, actual_received_requests): (
            HashSet<SocketAddr>,
            HashSet<RpcRequestEnvelope>,
        ) = actual_received_requests_by_peer.into_iter().unzip();

        assert_eq!(actual_receiving_peers, expected_receiving_peers);
        assert_eq!(actual_received_requests, expected_received_requests);
    }

    #[test_context(ClientReceivingAppendSuccess)]
    #[tokio::test]
    async fn emits_responses_from_peers_onto_channel(mut ctx: ClientReceivingAppendSuccess) {
        let _ = ctx
            .0
            .client
            .send_many(ctx.0.requests_by_peer.clone())
            .await
            .unwrap();

        let mut responses = Vec::new();
        while let Some((_, _, resp)) = ctx.0.response_rx.recv().await {
            responses.push(resp);
        }
        assert_eq!(responses, ctx.0.expected_responses.clone());
    }
}
