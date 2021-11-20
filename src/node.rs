use std::net::SocketAddr;
use std::sync::Arc;

use crate::api::client::{Client, ClientConfig};
use crate::api::request::Request::{Get, Put};
use crate::api::request::RequestEnvelope;
use crate::api::response::{Response, ResponseEnvelope};
use crate::api::server::{Server, ServerConfig};
use crate::error::PermissionError;
use crate::state::store::Store;
use crate::Result;

pub struct Node {
    pub address: SocketAddr,
    role: Arc<Role>, // arc b/c we need to share role across task boundaries. (refactor to avoid that?)
    client: Arc<Client>,
    server: Server,
    store: Arc<Store>,
}

pub struct NodeConfig {
    address: SocketAddr,
    peer_addresses: Vec<SocketAddr>,
    role: Role,
}

pub enum Role {
    Leader,
    Follower,
}

impl Node {
    pub fn new(cfg: NodeConfig) -> Node {
        let NodeConfig {
            address,
            peer_addresses,
            role,
        } = cfg;
        Self {
            address: address.clone(),
            role: Arc::new(role),
            client: Arc::new(Client::new(ClientConfig { peer_addresses })),
            server: Server::new(ServerConfig { address }),
            store: Arc::new(Store::new()),
        }
    }

    pub async fn run(&mut self) -> Result<()> {
        let _ = self.server.run().await;
        let _ = self.client.run().await?;

        let role = self.role.clone();
        let store = self.store.clone();
        let request_receiver = self.server.request_receiver.clone();
        let a_client = self.client.clone();

        tokio::spawn(async move {
            while let Some((envelope, responder)) = request_receiver.lock().await.recv().await {
                let RequestEnvelope { id, body: request } = envelope;

                let role = role.clone();
                let store = store.clone();
                let a_client = a_client.clone();

                let response: ResponseEnvelope = match request {
                    Get { key } => Node::handle_get(role, store, id, key).await,
                    Put { key, value } => {
                        Node::handle_set(role, store, a_client, id, key, value).await
                    }
                };
                let _ = responder.send(response)?;
            }
            Ok::<(), ResponseEnvelope>(())
        });
        Ok(())
    }

    async fn handle_get(
        role: Arc<Role>,
        store: Arc<Store>,
        id: u64,
        key: String,
    ) -> ResponseEnvelope {
        // TODO: avoid this branching by extracting `LeaderNode` and `FollowerNode` impls
        match role.as_ref() {
            Role::Follower => {
                ResponseEnvelope::error_of(id, PermissionError::FollowersMayNotGet.to_string())
            }
            Role::Leader => {
                let value = store.get(&key).await;
                ResponseEnvelope::of_get(id, value)
            }
        }
    }

    async fn handle_set(
        role: Arc<Role>,
        store: Arc<Store>,
        client: Arc<Client>,
        id: u64,
        key: String,
        value: String,
    ) -> ResponseEnvelope {
        // TODO: avoid this branching by extracting `LeaderNode` and `FollowerNode` impls
        match role.as_ref() {
            Role::Follower => {
                let was_modified = store.put(&key, &value).await;
                ResponseEnvelope::of_set(id, was_modified)
            }
            Role::Leader => {
                let request = Put {
                    key: key.clone(),
                    value: value.clone(),
                };
                let filter = |r: ResponseEnvelope| match r.body {
                    Response::ToPut { .. } => Some(r),
                    _ => None,
                };
                match client.broadcast_and_filter(request, filter).await {
                    Ok(_) => {
                        let was_modified = store.put(&key, &value).await;
                        ResponseEnvelope::of_set(id, was_modified.clone())
                    }
                    Err(e) => ResponseEnvelope::error_of(id, e.to_string()),
                }
            }
        }
    }
}

#[cfg(test)]
mod test_node {
    use tokio::net::TcpListener;

    use crate::api::request::Request;
    use crate::api::response::Response;
    use crate::api::ServerConnection;
    use crate::error::NetworkError::BroadcastFailure;
    use crate::error::PermissionError::FollowersMayNotGet;
    use crate::test_support::gen::Gen;

    use super::*;

    lazy_static! {
        static ref NUM_PEERS: usize = 5;
        static ref MAJORITY: usize = *NUM_PEERS / 2;
        static ref GET_REQ: Request = Request::Get {
            key: "foo".to_string(),
        };
        static ref PUT_REQ: Request = Request::Put {
            key: "foo".to_string(),
            value: "bar".to_string(),
        };
        static ref PUT_RESP: Response = Response::ToPut { was_modified: true };
        static ref ERR_RESP: Response = Response::Error {
            msg: "oh noes!".to_string()
        };
    }

    struct Runner {
        node_address: String,
        client: Client,
    }

    async fn setup_with(role: Role, responses: Vec<Response>) -> Runner {
        let responses = Arc::new(responses);
        let peer_addresses: Vec<SocketAddr> = (0..*NUM_PEERS).map(|_| Gen::socket_addr()).collect();

        for (peer_idx, peer_addr) in peer_addresses.clone().into_iter().enumerate() {
            let listener = TcpListener::bind(peer_addr).await.unwrap();
            let responses = responses.clone();

            tokio::spawn(async move {
                for _ in 0..*NUM_PEERS {
                    let (socket, _) = listener.accept().await.unwrap();
                    // println!("> Peer listening at {:?}", peer_addr);

                    let responses = responses.clone();
                    tokio::spawn(async move {
                        let conn = ServerConnection::new(socket);
                        loop {
                            let req = conn.read().await.unwrap();
                            // println!("> Peer at {:?} got request: {:?}", peer_addr, req);
                            if !responses.is_empty() {
                                let response = ResponseEnvelope {
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

        let mut node = Node::new(NodeConfig {
            address: Gen::socket_addr(),
            peer_addresses: peer_addresses.clone(),
            role,
        });
        let _ = node.run().await.unwrap();

        let client = Client::new(ClientConfig {
            peer_addresses: vec![node.address],
        });
        let _ = client.run().await.unwrap();

        return Runner {
            node_address: node.address.to_string(),
            client,
        };
    }

    #[tokio::test]
    async fn leader_handles_get_of_missing_value() {
        let Runner {
            node_address,
            client,
            ..
        } = setup_with(Role::Leader, vec![]).await;

        let response = client.write_one(&node_address, &GET_REQ).await.unwrap();

        assert_eq!(response.body, Response::ToGet { value: None });
    }

    #[tokio::test]
    async fn leader_handles_successfully_replicated_put() {
        let responses = std::iter::repeat(PUT_RESP.clone())
            .take(*NUM_PEERS)
            .collect::<Vec<Response>>();
        let Runner {
            node_address,
            client,
            ..
        } = setup_with(Role::Leader, responses).await;

        let response = client.write_one(&node_address, &PUT_REQ).await.unwrap();

        assert_eq!(response.body, Response::ToPut { was_modified: true });
    }

    #[tokio::test]
    async fn leader_handles_unsuccessfully_replicated_put() {
        let responses = std::iter::repeat(ERR_RESP.clone())
            .take(*NUM_PEERS)
            .collect::<Vec<Response>>();
        let Runner {
            node_address,
            client,
            ..
        } = setup_with(Role::Leader, responses).await;

        let put_response = client.write_one(&node_address, &PUT_REQ).await.unwrap();
        let get_response = client.write_one(&node_address, &GET_REQ).await.unwrap();

        assert_eq!(
            put_response.body,
            Response::Error {
                msg: BroadcastFailure.to_string()
            }
        );
        assert_eq!(get_response.body, Response::ToGet { value: None });
    }

    #[tokio::test]
    async fn leader_handles_timed_out_replication() {
        let Runner {
            node_address,
            client,
            ..
        } = setup_with(Role::Leader, vec![]).await;

        let response = client.write_one(&node_address, &PUT_REQ).await.unwrap();

        assert_eq!(
            response.body,
            Response::Error {
                msg: BroadcastFailure.to_string()
            }
        );
    }

    #[tokio::test]
    async fn leader_handles_get_of_put_value() {
        let responses = std::iter::repeat(PUT_RESP.clone())
            .take(*NUM_PEERS)
            .collect::<Vec<Response>>();
        let put_request = Request::Put {
            key: "foo".to_string(),
            value: "bar".to_string(),
        };
        let get_request = Request::Get {
            key: "foo".to_string(),
        };
        let Runner {
            node_address,
            client,
            ..
        } = setup_with(Role::Leader, responses).await;

        let _ = client.write_one(&node_address, &put_request).await.unwrap();
        let get_response = client.write_one(&node_address, &get_request).await.unwrap();
        assert_eq!(
            get_response.body,
            Response::ToGet {
                value: Some("bar".to_string())
            }
        );
    }

    #[tokio::test]
    async fn leader_handles_idempotent_puts() {
        let responses = std::iter::repeat(PUT_RESP.clone())
            .take(*NUM_PEERS)
            .collect::<Vec<Response>>();
        let put_request = Request::Put {
            key: "foo".to_string(),
            value: "bar".to_string(),
        };
        let Runner {
            node_address,
            client,
            ..
        } = setup_with(Role::Leader, responses).await;

        let put_response_1 = client.write_one(&node_address, &put_request).await.unwrap();
        let put_response_2 = client.write_one(&node_address, &put_request).await.unwrap();

        assert_eq!(put_response_1.body, Response::ToPut { was_modified: true });
        assert_eq!(
            put_response_2.body,
            Response::ToPut {
                was_modified: false
            }
        );
    }

    #[tokio::test]
    async fn leader_handles_sequential_puts() {
        let responses = std::iter::repeat(PUT_RESP.clone())
            .take(*NUM_PEERS)
            .collect::<Vec<Response>>();
        let put_request_1 = Request::Put {
            key: "foo".to_string(),
            value: "bar".to_string(),
        };
        let put_request_2 = Request::Put {
            key: "foo".to_string(),
            value: "baz".to_string(),
        };
        let get_request = Request::Get {
            key: "foo".to_string(),
        };

        let Runner {
            node_address,
            client,
            ..
        } = setup_with(Role::Leader, responses).await;

        let put_response_1 = client
            .write_one(&node_address, &put_request_1)
            .await
            .unwrap();
        let put_response_2 = client
            .write_one(&node_address, &put_request_2)
            .await
            .unwrap();
        let get_response = client.write_one(&node_address, &get_request).await.unwrap();

        assert_eq!(put_response_1.body, Response::ToPut { was_modified: true });
        assert_eq!(put_response_2.body, Response::ToPut { was_modified: true });
        assert_eq!(
            get_response.body,
            Response::ToGet {
                value: Some("baz".to_string()),
            }
        )
    }

    #[tokio::test]
    async fn folllower_rejects_get() {
        let Runner {
            node_address,
            client,
            ..
        } = setup_with(Role::Follower, vec![]).await;

        let get_response = client.write_one(&node_address, &GET_REQ).await.unwrap();
        assert_eq!(
            get_response.body,
            Response::Error {
                msg: FollowersMayNotGet.to_string()
            }
        )
    }

    #[tokio::test]
    async fn folllower_handles_put() {
        let Runner {
            node_address,
            client,
            ..
        } = setup_with(Role::Follower, vec![]).await;

        let put_response = client.write_one(&node_address, &PUT_REQ).await.unwrap();
        assert_eq!(put_response.body, Response::ToPut { was_modified: true });
    }
}
