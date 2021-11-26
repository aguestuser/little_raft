use std::net::SocketAddr;
use std::sync::Arc;

use tokio::sync::mpsc::Receiver;
use tokio::sync::{mpsc, oneshot};
use tokio::time::{sleep, Duration};

use crate::api::request::{ApiRequest, ApiRequestEnvelope};
use crate::api::response::ApiResponseEnvelope;
use crate::api::server::{ApiServer, ApiServerConfig, RespondableApiRequest};
use crate::error::ProtocolError::LogReplicationFailure;
use crate::error::Result;
use crate::rpc::client::{RpcClient, RpcClientConfig, RpcResponseInContext};
use crate::rpc::request::{RpcRequest, RpcRequestEnvelope};
use crate::rpc::response::{RpcResponse, RpcResponseEnvelope};
use crate::rpc::server::{RespondableRpcRequest, RpcServer, RpcServerConfig};
use crate::state::log::Command;
use crate::state::{State, StateConfig};
use crate::NodeAddr;
use crate::CHAN_BUF_SIZE;

#[cfg(not(test))]
pub const HEARTBEAT_INTERVAL_IN_MILLIS: u64 = 200;
#[cfg(test)]
pub const HEARTBEAT_INTERVAL_IN_MILLIS: u64 = 2;
#[cfg(not(test))]
pub const API_PUT_TIMEOUT_IN_MILLIS: u64 = 5 * 1000 * 60; // 5 min
#[cfg(test)]
pub const API_PUT_TIMEOUT_IN_MILLIS: u64 = 50;

pub enum Role {
    Leader,
    Follower,
}

pub struct NodeConfig {
    role: Role,
    api_address: SocketAddr, // TODO: make these strings that get converted to SocketAddr in `run`
    rpc_address: SocketAddr, // same
    leader_address: NodeAddr,
    peer_addresses: Vec<SocketAddr>,
    log_path: String,
    metadata_path: String,
}

#[allow(unused)]
pub struct Node {
    role: Arc<Role>, // TODO: eliminate `role` field in favor of `LeaderNode`/`FollowerNode`/`CandidateNode` variants
    api_server: Arc<ApiServer>,
    rpc_client: Arc<RpcClient>,
    rpc_server: Arc<RpcServer>,
    state: Arc<State>,
}

impl Role {
    pub fn is_leader(&self) -> bool {
        match self {
            Role::Leader => true,
            _ => false,
        }
    }
}

impl NodeConfig {
    pub async fn run(self) -> Result<Node> {
        let api_server_config = ApiServerConfig {
            address: self.api_address,
        };
        let rpc_server_config = RpcServerConfig {
            address: self.rpc_address.clone(),
        };
        let rpc_client_config = RpcClientConfig {
            peer_addresses: self.peer_addresses.clone(),
        };
        let state_config = StateConfig {
            leader_address: self.leader_address,
            node_address: self.rpc_address.to_string(),
            peer_addresses: self
                .peer_addresses
                .into_iter()
                .map(|pa| pa.to_string())
                .collect(),
            log_path: self.log_path,
            metadata_path: self.metadata_path,
        };

        let (rpc_request_tx, rpc_request_rx) =
            mpsc::channel::<RespondableRpcRequest>(CHAN_BUF_SIZE);
        let (rpc_response_tx, rpc_response_rx) =
            mpsc::channel::<RpcResponseInContext>(CHAN_BUF_SIZE);
        let (api_request_tx, api_request_rx) =
            mpsc::channel::<RespondableApiRequest>(CHAN_BUF_SIZE);

        let role = Arc::new(self.role);
        let state = Arc::new(state_config.run().await?);
        let rpc_server = Arc::new(rpc_server_config.run_with(rpc_request_tx).await?);
        let rpc_client = Arc::new(rpc_client_config.run_with(rpc_response_tx).await?);
        let api_server = Arc::new(api_server_config.run_with(api_request_tx).await?);

        Node::handle_rpc_requests(rpc_request_rx, role.clone(), state.clone());
        Node::handle_rpc_responses(rpc_response_rx, state.clone());
        Node::handle_api_requests(
            api_request_rx,
            rpc_client.clone(),
            role.clone(),
            state.clone(),
        );

        if role.is_leader() {
            Node::run_heartbeat(rpc_client.clone(), state.clone());
        }

        Ok(Node {
            role,
            api_server,
            rpc_client,
            rpc_server,
            state,
        })
    }
}

impl Node {
    /// Handle api requests (which may be either `Get` or `Put` commands) from clients in a loop.
    ///
    /// All nodes respond to `Get` requests by reading whatever value is currently stored in the
    /// state machine for the given key.
    ///
    /// Leaders handle `Put` by attempting to replicate the command to all follower logs, waiting
    /// to respond until a majority of followers have committed the command, causing the leader to
    /// apply the command to its state machine. If the log replication fails or times out, leader
    /// responds with failure so client may retry. If it succeeds, we respond indicating whether the
    /// key that was put to the state machine modified a previous value or not.
    ///
    /// Followers handle `Put` by redirecting to the leader so client may retry.
    pub fn handle_api_requests(
        mut api_request_rx: Receiver<RespondableApiRequest>,
        rpc_client: Arc<RpcClient>,
        role: Arc<Role>,
        state: Arc<State>,
    ) {
        tokio::spawn(async move {
            while let Some((ApiRequestEnvelope { id, request }, responder)) =
                api_request_rx.recv().await
            {
                let response: ApiResponseEnvelope = match request {
                    ApiRequest::Get { key } => {
                        let value = state.fetch_from_store(&key).await;
                        ApiResponseEnvelope::of_get(id, value)
                    }
                    ApiRequest::Put { key, value } => match role.as_ref() {
                        Role::Leader => {
                            let is_modification =
                                state.fetch_from_store(&key).await != Some(value.clone());
                            if let Ok(log_index) =
                                state.append_to_log(Command::Put { key, value }).await
                            {
                                // Register a callback that will be called in `State::apply_all_until`.
                                // Trigger an attempt to sync logs, return when callback is triggered
                                // (indicating this command has been successfully replicated) or times out.
                                let (on_apply_tx, on_apply_rx) = oneshot::channel::<()>();
                                state.register_on_apply_handler(log_index, on_apply_tx);
                                let _ = Self::sync_logs(rpc_client.clone(), state.clone()).await;
                                tokio::select! {
                                    result = on_apply_rx => {
                                        if result.is_ok() {
                                            ApiResponseEnvelope::of_put(id, is_modification)
                                        } else {
                                            ApiResponseEnvelope::error_of(id, LogReplicationFailure.to_string())
                                        }
                                    }
                                    _ = sleep(Duration::from_millis(API_PUT_TIMEOUT_IN_MILLIS)) => {
                                        ApiResponseEnvelope::error_of(id, LogReplicationFailure.to_string())
                                    }
                                }
                            } else {
                                ApiResponseEnvelope::error_of(id, LogReplicationFailure.to_string())
                            }
                        }
                        Role::Follower => {
                            ApiResponseEnvelope::of_redirect(id, state.get_leader_address().await)
                        }
                    },
                };

                let _ = responder.send(response)?;
            }
            Ok::<(), ApiResponseEnvelope>(())
        });
    }

    /// (LEADERS ONLY)
    /// Attempt to sync log entries with followers by issuing an `AppendEntryRequest`
    /// to each follower containing log entries ranging from the last index known to be committed by
    /// that follower up to the last index known to be appended to the leader's log.  
    pub async fn sync_logs(rpc_client: Arc<RpcClient>, state: Arc<State>) {
        //let last_appended_index = state.get_last_appended_index().await;
        let requests = state.gen_append_entry_requests().await;
        let _ = rpc_client.send_many(requests).await;
    }

    /// (LEADERS ONLY)
    /// Attempt to sync log entries with followers on a specified interval
    pub fn run_heartbeat(rpc_client: Arc<RpcClient>, state: Arc<State>) {
        tokio::spawn(async move {
            loop {
                sleep(Duration::from_millis(HEARTBEAT_INTERVAL_IN_MILLIS)).await;
                let _ = Self::sync_logs(rpc_client.clone(), state.clone()).await;
            }
        });
    }

    /// (ALL NODES)
    /// Listen for `RespondableRpcRequest` tuples emitted from the `RpcServer` and handle them
    /// appropriately according to the node's `role` to modify its current `state`.
    /// For followers: handle `AppendEntries` requests from leaders, and issue reponses indicating
    /// whether the call succeeded and the value of the follower's current term.
    pub fn handle_rpc_requests(
        mut request_rx: Receiver<RespondableRpcRequest>,
        role: Arc<Role>,
        state: Arc<State>,
    ) {
        tokio::spawn(async move {
            while let Some((request_envelope, responder)) = request_rx.recv().await {
                let RpcRequestEnvelope { id, request } = request_envelope;
                match request {
                    RpcRequest::AppendEntries(req) => match role.as_ref() {
                        Role::Follower => {
                            let response = state.handle_append_entries_request(req).await;
                            let _ =
                                responder.send(RpcResponseEnvelope::of_append_entry(id, response));
                        }
                        Role::Leader => {}
                    },
                }
            }
        });
    }

    /// (ALL NODES)
    /// Listen for `RpcResponseInContext` 3-tuples emitted by the `RpcClient` and use them to
    /// modify the node's `state`.
    fn handle_rpc_responses(
        mut rpc_response_rx: Receiver<RpcResponseInContext>,
        state: Arc<State>,
    ) {
        tokio::spawn(async move {
            while let Some((peer_addr, request, response)) = rpc_response_rx.recv().await {
                // println!(
                //     "Node got response from peer at {:?}\n-- req: {:?}\n-- res: {:?} ",
                //     peer_addr.clone(),
                //     request.clone(),
                //     response.clone(),
                // );
                match (request, response) {
                    (RpcRequest::AppendEntries(req), RpcResponse::ToAppendEntries(resp)) => {
                        let _ = state
                            .handle_append_entry_response(peer_addr, req, resp)
                            .await;
                    }
                }
            }
        });
    }
}

#[cfg(test)]
mod test_node {
    use test_context::{test_context, AsyncTestContext};
    use tokio::fs;
    use tokio::net::TcpListener;

    use crate::api::client::{ApiClient, ApiClientConfig};
    use crate::error::ProtocolError::{LeaderRequired, ServerError};
    use crate::rpc::request::AppendEntriesRequest;
    use crate::rpc::response::{AppendEntriesResponse, RpcResponse};
    use crate::rpc::RpcServerConnection;
    use crate::state::log::LogEntry;
    use crate::test_support::gen::Gen;

    use super::*;

    lazy_static! {
        static ref NUM_NODES: usize = 5;
        static ref NUM_PEERS: usize = 4;
        static ref MAJORITY: usize = *NUM_PEERS / 2 + *NUM_PEERS % 2;
        static ref PUT_CMD: Command = Command::Put {
            key: "foo".to_string(),
            value: "bar".to_string()
        };
        static ref APPEND_SUCCESS: RpcResponse =
            RpcResponse::ToAppendEntries(AppendEntriesResponse {
                peer_term: 0,
                success: true,
            });
        static ref APPEND_FAILURE: RpcResponse =
            RpcResponse::ToAppendEntries(AppendEntriesResponse {
                peer_term: 0,
                success: false,
            });
        static ref APPEND_SUCCESS_FROM_ALL_PEERS: Vec<RpcResponse> =
            std::iter::repeat(APPEND_SUCCESS.clone())
                .take(*NUM_PEERS)
                .collect::<Vec<RpcResponse>>();
        static ref APPEND_FAILURE_FROM_ALL_PEERS: Vec<RpcResponse> =
            std::iter::repeat(APPEND_FAILURE.clone())
                .take(*NUM_PEERS)
                .collect::<Vec<RpcResponse>>();
    }

    struct Context {
        client: ApiClient,
        leader_address: NodeAddr,
        log_path: String,
        metadata_path: String,
    }

    impl Context {
        async fn setup(role: Role, responses: Vec<RpcResponse>, entries: Vec<LogEntry>) -> Context {
            let responses = Arc::new(responses);
            let node_addresses = (0..*NUM_NODES)
                .map(|_| Gen::socket_addr())
                .collect::<Vec<SocketAddr>>();
            let own_address = node_addresses[0].clone();
            let peer_addresses = node_addresses[1..node_addresses.len()].to_vec();
            let leader_address = match role {
                Role::Leader => own_address.to_string(),
                _ => peer_addresses[0].clone().to_string(),
            };

            for (peer_idx, peer_addr) in peer_addresses.clone().into_iter().enumerate() {
                let listener = TcpListener::bind(peer_addr).await.unwrap();
                let responses = responses.clone();

                tokio::spawn(async move {
                    for _ in 0..*NUM_PEERS {
                        let (socket, _) = listener.accept().await.unwrap();
                        // println!("> Peer RpcServer listening at {:?}", peer_addr);

                        let responses = responses.clone();
                        tokio::spawn(async move {
                            let conn = RpcServerConnection::new(socket);
                            loop {
                                let req = conn.read().await.unwrap();
                                // println!("> Peer at {:?} got request: {:?}", peer_addr.clone(), req);
                                if !responses.is_empty() {
                                    let response = RpcResponseEnvelope {
                                        id: req.id,
                                        response: responses[peer_idx].clone(),
                                    };
                                    // println!("> Peer {:?} sending {:?}", peer_addr, response.clone());
                                    conn.write(response).await.unwrap();
                                }
                            }
                        });
                    }
                });
            }

            let log_path = format!("test_data/log_{}", Gen::usize().to_string());
            let metadata_path = format!("test_data/metadata_{}", Gen::usize().to_string());
            fs::create_dir(metadata_path.clone()).await.unwrap();

            let api_address = Gen::socket_addr();
            let node_config = NodeConfig {
                role,
                api_address: api_address.clone(),
                rpc_address: own_address,
                leader_address: leader_address.clone(),
                peer_addresses,
                log_path: log_path.clone(),
                metadata_path: metadata_path.clone(),
            };
            let client_config = ApiClientConfig {
                server_address: api_address,
            };

            let node = node_config.run().await.unwrap();
            if !entries.is_empty() {
                let leader_commit = entries.len();
                let _ = node
                    .state
                    .handle_append_entries_request(AppendEntriesRequest {
                        entries,
                        leader_address: leader_address.clone(),
                        leader_commit,
                        leader_term: 0,
                        prev_log_index: 0,
                        prev_log_term: 0,
                    })
                    .await;
            }

            let client = client_config.run().await.unwrap();

            return Context {
                client,
                leader_address,
                log_path,
                metadata_path,
            };
        }

        pub async fn teardown(self) {
            // TODO: shut down node server/clients
            let _ = tokio::fs::remove_file(self.log_path).await.unwrap();
            let _ = tokio::fs::remove_dir_all(self.metadata_path).await.unwrap();
        }
    }

    struct Leader(Context);
    #[async_trait::async_trait]
    impl AsyncTestContext for Leader {
        async fn setup() -> Self {
            let ctx = Context::setup(Role::Leader, vec![], vec![]).await;
            Leader(ctx)
        }
        async fn teardown(self) {
            self.0.teardown().await
        }
    }

    struct LeaderWithEntries(Context);
    #[async_trait::async_trait]
    impl AsyncTestContext for LeaderWithEntries {
        async fn setup() -> Self {
            let ctx = Context::setup(
                Role::Leader,
                vec![],
                vec![LogEntry {
                    term: 0,
                    command: PUT_CMD.clone(),
                }],
            )
            .await;
            LeaderWithEntries(ctx)
        }
        async fn teardown(self) {
            self.0.teardown().await
        }
    }

    struct LeaderWithSuccessFromAllPeers(Context);
    #[async_trait::async_trait]
    impl AsyncTestContext for LeaderWithSuccessFromAllPeers {
        async fn setup() -> Self {
            let ctx =
                Context::setup(Role::Leader, APPEND_SUCCESS_FROM_ALL_PEERS.clone(), vec![]).await;
            LeaderWithSuccessFromAllPeers(ctx)
        }
        async fn teardown(self) {
            self.0.teardown().await
        }
    }

    struct LeaderWithFailureFromAllPeers(Context);
    #[async_trait::async_trait]
    impl AsyncTestContext for LeaderWithFailureFromAllPeers {
        async fn setup() -> Self {
            let ctx =
                Context::setup(Role::Leader, APPEND_FAILURE_FROM_ALL_PEERS.clone(), vec![]).await;
            LeaderWithFailureFromAllPeers(ctx)
        }
        async fn teardown(self) {
            self.0.teardown().await
        }
    }

    struct Follower(Context);
    #[async_trait::async_trait]
    impl AsyncTestContext for Follower {
        async fn setup() -> Self {
            let ctx = Context::setup(Role::Follower, vec![], vec![]).await;
            Follower(ctx)
        }
        async fn teardown(self) {
            self.0.teardown().await
        }
    }

    struct FollowerWithEntries(Context);
    #[async_trait::async_trait]
    impl AsyncTestContext for FollowerWithEntries {
        async fn setup() -> Self {
            let ctx = Context::setup(
                Role::Follower,
                vec![],
                vec![LogEntry {
                    term: 0,
                    command: PUT_CMD.clone(),
                }],
            )
            .await;
            FollowerWithEntries(ctx)
        }
        async fn teardown(self) {
            self.0.teardown().await
        }
    }

    #[cfg(test)]
    mod leader {
        use super::*;

        #[test_context(Leader)]
        #[tokio::test]
        async fn handles_get_of_missing_value(ctx: Leader) {
            let response = ctx.0.client.get("foo").await.unwrap();
            assert_eq!(response, None);
        }

        #[test_context(LeaderWithEntries)]
        #[tokio::test]
        async fn handles_get_of_previously_replicated_value(ctx: LeaderWithEntries) {
            let response = ctx.0.client.get("foo").await.unwrap();
            assert_eq!(response, Some("bar".to_string()));
        }

        #[test_context(LeaderWithSuccessFromAllPeers)]
        #[tokio::test]
        async fn handles_successfully_replicated_put(ctx: LeaderWithSuccessFromAllPeers) {
            let response = ctx.0.client.put("foo", "bar").await.unwrap();
            assert_eq!(response, true);
        }

        #[test_context(LeaderWithFailureFromAllPeers)]
        #[tokio::test]
        async fn handles_unsuccessfully_replicated_put(ctx: LeaderWithFailureFromAllPeers) {
            let put_response = ctx.0.client.put("foo", "bar").await;
            let get_response = ctx.0.client.get("foo").await.unwrap();

            assert_eq!(
                put_response.err().unwrap().to_string(),
                ServerError(LogReplicationFailure.to_string()).to_string(),
            );
            assert_eq!(get_response, None);
        }

        #[test_context(Leader)]
        #[tokio::test]
        async fn handles_timed_out_replication(ctx: Leader) {
            let response = ctx.0.client.put("foo", "bar").await;
            assert_eq!(
                response.err().unwrap().to_string(),
                ServerError(LogReplicationFailure.to_string()).to_string(),
            );
        }

        #[test_context(LeaderWithSuccessFromAllPeers)]
        #[tokio::test]
        async fn handles_get_of_put_value(ctx: LeaderWithSuccessFromAllPeers) {
            let _ = ctx.0.client.put("foo", "bar").await;
            let get_response = ctx.0.client.get("foo").await.unwrap();
            assert_eq!(get_response, Some("bar".to_string()));
        }

        #[test_context(LeaderWithSuccessFromAllPeers)]
        #[tokio::test]
        async fn handles_idempotent_puts(ctx: LeaderWithSuccessFromAllPeers) {
            let put_response_1 = ctx.0.client.put("foo", "bar").await.unwrap();
            let put_response_2 = ctx.0.client.put("foo", "bar").await.unwrap();

            assert_eq!(put_response_1, true);
            assert_eq!(put_response_2, false);
        }

        #[test_context(LeaderWithSuccessFromAllPeers)]
        #[tokio::test]
        async fn handles_sequential_puts(ctx: LeaderWithSuccessFromAllPeers) {
            let put_response_1 = ctx.0.client.put("foo", "bar").await.unwrap();
            let put_response_2 = ctx.0.client.put("foo", "baz").await.unwrap();

            let get_response = ctx.0.client.get("foo").await.unwrap();

            assert_eq!(put_response_1, true);
            assert_eq!(put_response_2, true);
            assert_eq!(get_response, Some("baz".to_string()));
        }
    }

    #[cfg(test)]
    mod follower {
        use super::*;

        #[test_context(FollowerWithEntries)]
        #[tokio::test]
        async fn handles_get(ctx: FollowerWithEntries) {
            assert_eq!(
                ctx.0.client.get("foo").await.unwrap(),
                Some("bar".to_string())
            );
        }

        #[test_context(Follower)]
        #[tokio::test]
        async fn redirects_put(ctx: Follower) {
            let put_response = ctx.0.client.put("foo", "bar").await;
            assert_eq!(
                put_response.err().unwrap().to_string(),
                LeaderRequired(ctx.0.leader_address.clone()).to_string(),
            );
        }
    }
}
