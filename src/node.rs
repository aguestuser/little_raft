use std::net::SocketAddr;
use std::sync::Arc;

use crate::api::request::ApiRequest;
use crate::api::response::ApiResponseEnvelope;
use crate::api::server::ApiServer;
use crate::error::ProtocolError::{FollowerRequired, ReplicationFailed};
use crate::error::Result;
use crate::rpc_legacy::client::{RpcClient, RpcClientConfig};
use crate::rpc_legacy::response::RpcResponseEnvelope;
use crate::rpc_legacy::server::RpcServer;
use crate::rpc_legacy::RpcRequest;
use crate::state::store::Store;
use crate::tcp::ServerConfig;
use tokio::sync::Mutex;

pub struct Node {
    pub store: Arc<Store>,
    role: Arc<Role>, // arc b/c we need to share role across task boundaries. (refactor to avoid that?)
    api_server: ApiServer,
    leader_address: Arc<Mutex<SocketAddr>>,
    rpc_client: Arc<RpcClient>,
    rpc_server: RpcServer,
}

pub struct NodeConfig {
    role: Role,
    api_address: SocketAddr,
    rpc_address: SocketAddr,
    leader_address: SocketAddr,
    peer_addresses: Vec<SocketAddr>,
}

pub enum Role {
    Leader,
    Follower,
}

impl Node {
    pub fn new(cfg: NodeConfig) -> Node {
        let NodeConfig {
            api_address,
            rpc_address,
            leader_address,
            peer_addresses,
            role,
        } = cfg;
        Self {
            role: Arc::new(role),
            store: Arc::new(Store::new()),
            api_server: ApiServer::new(ServerConfig {
                address: api_address,
            }),
            leader_address: Arc::new(Mutex::new(leader_address)),
            rpc_client: Arc::new(RpcClient::new(RpcClientConfig { peer_addresses })),
            rpc_server: RpcServer::new(ServerConfig {
                address: rpc_address,
            }),
        }
    }

    pub async fn run(&mut self) -> Result<()> {
        let mut rpc_request_rx = self.rpc_server.run().await?;
        let _ = self.rpc_client.run().await?;

        // handle api requests
        let mut api_request_rx = self.api_server.run().await?;
        let role = self.role.clone();
        let store = self.store.clone();
        let rpc_client = self.rpc_client.clone();
        let leader_address = self.leader_address.clone();

        tokio::spawn(async move {
            while let Some((req, responder)) = api_request_rx.recv().await {
                let response: ApiResponseEnvelope = match req.body {
                    ApiRequest::Get { key } => {
                        let value = store.get(&key).await;
                        ApiResponseEnvelope::of_get(req.id, value)
                    }

                    ApiRequest::Put { key, value } => match role.as_ref() {
                        Role::Leader => {
                            if rpc_client
                                .replicate_put(key.clone(), value.clone())
                                .await
                                .is_ok()
                            {
                                ApiResponseEnvelope::of_put(req.id, store.put(&key, &value).await)
                            } else {
                                ApiResponseEnvelope::error_of(req.id, ReplicationFailed.to_string())
                            }
                        }

                        Role::Follower => ApiResponseEnvelope::of_redirect(
                            req.id,
                            leader_address.lock().await.to_string(),
                        ),
                    },
                };

                let _ = responder.send(response)?;
            }
            Ok::<(), ApiResponseEnvelope>(())
        });

        // handle rpc requests
        let role = self.role.clone();
        let store = self.store.clone();

        tokio::spawn(async move {
            while let Some((req, responder)) = rpc_request_rx.recv().await {
                let response = match req.body {
                    RpcRequest::Put { key, value } => match role.as_ref() {
                        Role::Leader => {
                            RpcResponseEnvelope::error_of(req.id, FollowerRequired.to_string())
                        }
                        Role::Follower => {
                            let was_modified = store.put(&key, &value).await;
                            RpcResponseEnvelope::of_put(req.id, was_modified)
                        }
                    },
                };
                let _ = responder.send(response)?;
            }
            Ok::<(), RpcResponseEnvelope>(())
        });

        Ok(())
    }
}

#[cfg(test)]
mod test_node {
    use tokio::net::TcpListener;

    use crate::rpc_legacy::response::RpcResponse;
    use crate::rpc_legacy::RpcServerConnection;
    use crate::test_support::gen::Gen;

    use super::*;
    use crate::api::client::{ApiClient, ApiClientConfig};
    use crate::error::ProtocolError::{LeaderRequired, ServerError};

    lazy_static! {
        static ref NUM_PEERS: usize = 5;
        static ref MAJORITY: usize = *NUM_PEERS / 2;
        static ref PUT_RESP: RpcResponse = RpcResponse::ToPut { was_modified: true };
        static ref ERR_RESP: RpcResponse = RpcResponse::ServerError {
            msg: "oh noes!".to_string()
        };
    }

    struct Runner {
        client: ApiClient,
        node: Node,
    }

    async fn setup_with(role: Role, responses: Vec<RpcResponse>) -> Runner {
        let responses = Arc::new(responses);
        let peer_addresses: Vec<SocketAddr> = (0..*NUM_PEERS).map(|_| Gen::socket_addr()).collect();
        let leader_address = peer_addresses[0].clone();

        for (peer_idx, peer_addr) in peer_addresses.clone().into_iter().enumerate() {
            let listener = TcpListener::bind(peer_addr).await.unwrap();
            let responses = responses.clone();

            tokio::spawn(async move {
                for _ in 0..*NUM_PEERS {
                    let (socket, _) = listener.accept().await.unwrap();
                    // println!("> Peer listening at {:?}", peer_addr);

                    let responses = responses.clone();
                    tokio::spawn(async move {
                        let conn = RpcServerConnection::new(socket);
                        loop {
                            let req = conn.read().await.unwrap();
                            // println!("> Peer at {:?} got request: {:?}", peer_addr, req);
                            if !responses.is_empty() {
                                let response = RpcResponseEnvelope {
                                    id: req.id,
                                    body: responses[peer_idx].clone(),
                                };
                                conn.write(response).await.unwrap();
                            }
                        }
                    });
                }
            });
        }

        let api_address = Gen::socket_addr();
        let mut node = Node::new(NodeConfig {
            api_address: api_address.clone(),
            rpc_address: Gen::socket_addr(),
            leader_address,
            peer_addresses: peer_addresses.clone(),
            role,
        });
        let _ = node.run().await.unwrap();

        let mut client = ApiClient::new(ApiClientConfig {
            server_address: api_address,
        });
        let _ = client.run().await.unwrap();

        return Runner { client, node };
    }

    #[tokio::test]
    async fn leader_handles_get_of_missing_value() {
        let Runner { client, .. } = setup_with(Role::Leader, vec![]).await;

        let response = client.get("foo").await.unwrap();

        assert_eq!(response, None);
    }

    #[tokio::test]
    async fn leader_handles_successfully_replicated_put() {
        let responses = std::iter::repeat(PUT_RESP.clone())
            .take(*NUM_PEERS)
            .collect::<Vec<RpcResponse>>();
        let Runner { client, .. } = setup_with(Role::Leader, responses).await;

        let response = client.put("foo", "bar").await.unwrap();
        assert_eq!(response, true);
    }

    #[tokio::test]
    async fn leader_handles_unsuccessfully_replicated_put() {
        let responses = std::iter::repeat(ERR_RESP.clone())
            .take(*NUM_PEERS)
            .collect::<Vec<RpcResponse>>();
        let Runner { client, .. } = setup_with(Role::Leader, responses).await;

        let put_response = client.put("foo", "bar").await;
        let get_response = client.get("foo").await.unwrap();

        assert_eq!(
            put_response.err().unwrap().to_string(),
            ServerError(ReplicationFailed.to_string()).to_string(),
        );
        assert_eq!(get_response, None);
    }

    #[tokio::test]
    async fn leader_handles_timed_out_replication() {
        let Runner { client, .. } = setup_with(Role::Leader, vec![]).await;

        let response = client.put("foo", "bar").await;
        assert_eq!(
            response.err().unwrap().to_string(),
            ServerError(ReplicationFailed.to_string()).to_string(),
        );
    }

    #[tokio::test]
    async fn leader_handles_get_of_put_value() {
        let responses = std::iter::repeat(PUT_RESP.clone())
            .take(*NUM_PEERS)
            .collect::<Vec<RpcResponse>>();
        let Runner { client, .. } = setup_with(Role::Leader, responses).await;

        let _ = client.put("foo", "bar").await;
        let get_response = client.get("foo").await.unwrap();
        assert_eq!(get_response, Some("bar".to_string()));
    }

    #[tokio::test]
    async fn leader_handles_idempotent_puts() {
        let responses = std::iter::repeat(PUT_RESP.clone())
            .take(*NUM_PEERS)
            .collect::<Vec<RpcResponse>>();
        let Runner { client, .. } = setup_with(Role::Leader, responses).await;

        let put_response_1 = client.put("foo", "bar").await.unwrap();
        let put_response_2 = client.put("foo", "bar").await.unwrap();

        assert_eq!(put_response_1, true);
        assert_eq!(put_response_2, false);
    }

    #[tokio::test]
    async fn leader_handles_sequential_puts() {
        let responses = std::iter::repeat(PUT_RESP.clone())
            .take(*NUM_PEERS)
            .collect::<Vec<RpcResponse>>();

        let Runner { client, .. } = setup_with(Role::Leader, responses).await;
        let put_response_1 = client.put("foo", "bar").await.unwrap();
        let put_response_2 = client.put("foo", "baz").await.unwrap();

        let get_response = client.get("foo").await.unwrap();

        assert_eq!(put_response_1, true);
        assert_eq!(put_response_2, true);
        assert_eq!(get_response, Some("baz".to_string()));
    }

    #[tokio::test]
    async fn follower_handles_get() {
        let Runner { client, node } = setup_with(Role::Follower, vec![]).await;
        let _ = node.store.clone().put(&"foo", &"bar").await;
        let get_response = client.get("foo").await.unwrap();
        assert_eq!(get_response, Some("bar".to_string()));
    }

    #[tokio::test]
    async fn folllower_redirects_put() {
        let Runner { client, node } = setup_with(Role::Follower, vec![]).await;
        let put_response = client.put("foo", "bar").await;
        let leader_address = node.leader_address.lock().await.to_string();
        assert_eq!(
            put_response.err().unwrap().to_string(),
            LeaderRequired(leader_address).to_string(),
        );
    }
}
