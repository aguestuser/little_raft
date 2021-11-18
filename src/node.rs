use crate::error::{PermissionError, Result};
use crate::store::Store;
use crate::tcp::client::{Client, ClientConfig};
use crate::tcp::request::Command::{Get, Invalid, Put};
use crate::tcp::request::Request;
use crate::tcp::response::{Outcome, Response};
use crate::tcp::server::{Server, ServerConfig};
use std::net::SocketAddr;
use std::sync::Arc;

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
            while let Some((req, responder)) = request_receiver.lock().await.recv().await {
                let Request { id, command } = req;

                let role = role.clone();
                let store = store.clone();
                let a_client = a_client.clone();

                let response: Response = match command {
                    Get { key } => Node::handle_get(role, store, id, key).await,
                    Put { key, value } => {
                        Node::handle_set(role, store, a_client, id, key, value).await
                    }
                    Invalid { msg } => Response::error_of(id, msg),
                };
                let _ = responder.send(response)?;
            }
            Ok::<(), Response>(())
        });
        Ok(())
    }

    async fn handle_get(role: Arc<Role>, store: Arc<Store>, id: u64, key: String) -> Response {
        // TODO: avoid this branching by extracting `LeaderNode` and `FollowerNode` impls
        match role.as_ref() {
            Role::Follower => {
                Response::error_of(id, PermissionError::FollowersMayNotGet.to_string())
            }
            Role::Leader => {
                let value = store.get(&key).await;
                Response::of_get(id, value)
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
    ) -> Response {
        // TODO: avoid this branching by extracting `LeaderNode` and `FollowerNode` impls
        match role.as_ref() {
            Role::Follower => {
                let was_modified = store.put(&key, &value).await;
                Response::of_set(id, was_modified)
            }
            Role::Leader => {
                let command = Put {
                    key: key.clone(),
                    value: value.clone(),
                };
                let filter = |r: Response| match r.outcome {
                    Outcome::OfPut { .. } => Some(r),
                    _ => None,
                };
                match client.broadcast_and_filter(command, filter).await {
                    Ok(_) => {
                        let was_modified = store.put(&key, &value).await;
                        Response::of_set(id, was_modified.clone())
                    }
                    Err(e) => Response::error_of(id, e.to_string()),
                }
            }
        }
    }
}

#[cfg(test)]
mod test_node {
    use super::*;
    use crate::error::NetworkError::BroadcastFailure;
    use crate::error::PermissionError::FollowersMayNotGet;
    use crate::tcp::connection::ServerConnection;
    use crate::tcp::request::Command;
    use crate::tcp::response::Outcome;
    use crate::test_support::gen::Gen;
    use tokio::net::TcpListener;

    lazy_static! {
        static ref NUM_PEERS: usize = 5;
        static ref MAJORITY: usize = *NUM_PEERS / 2;
        static ref GET_CMD: Command = Command::Get {
            key: "foo".to_string(),
        };
        static ref PUT_CMD: Command = Command::Put {
            key: "foo".to_string(),
            value: "bar".to_string(),
        };
        static ref PUT_OUTCOME: Outcome = Outcome::OfPut { was_modified: true };
        static ref ERROR_OUTCOME: Outcome = Outcome::Error {
            msg: "oh noes!".to_string()
        };
    }

    struct Runner {
        node_address: String,
        client: Client,
    }

    async fn setup_with(role: Role, outcomes: Vec<Outcome>) -> Runner {
        let outcomes = Arc::new(outcomes);
        let peer_addresses: Vec<SocketAddr> = (0..*NUM_PEERS).map(|_| Gen::socket_addr()).collect();

        for (peer_idx, peer_addr) in peer_addresses.clone().into_iter().enumerate() {
            let listener = TcpListener::bind(peer_addr).await.unwrap();
            let outcomes = outcomes.clone();

            tokio::spawn(async move {
                for _ in 0..*NUM_PEERS {
                    let (socket, _) = listener.accept().await.unwrap();
                    // println!("> Peer listening at {:?}", peer_addr);

                    let outcomes = outcomes.clone();
                    tokio::spawn(async move {
                        let conn = ServerConnection::new(socket);
                        loop {
                            let req = conn.read().await.unwrap();
                            // println!("> Peer at {:?} got request: {:?}", peer_addr, req);
                            if !outcomes.is_empty() {
                                let response = Response {
                                    id: req.id,
                                    outcome: outcomes[peer_idx].clone(),
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

        let response = client.write_one(&node_address, &GET_CMD).await.unwrap();

        assert_eq!(response.outcome, Outcome::OfGet { value: None });
    }

    #[tokio::test]
    async fn leader_handles_successfully_replicated_put() {
        let outcomes = std::iter::repeat(PUT_OUTCOME.clone())
            .take(*NUM_PEERS)
            .collect::<Vec<Outcome>>();
        let Runner {
            node_address,
            client,
            ..
        } = setup_with(Role::Leader, outcomes).await;

        let response = client.write_one(&node_address, &PUT_CMD).await.unwrap();

        assert_eq!(response.outcome, Outcome::OfPut { was_modified: true });
    }

    #[tokio::test]
    async fn leader_handles_unsuccessfully_replicated_put() {
        let outcomes = std::iter::repeat(ERROR_OUTCOME.clone())
            .take(*NUM_PEERS)
            .collect::<Vec<Outcome>>();
        let Runner {
            node_address,
            client,
            ..
        } = setup_with(Role::Leader, outcomes).await;

        let put_response = client.write_one(&node_address, &PUT_CMD).await.unwrap();
        let get_response = client.write_one(&node_address, &GET_CMD).await.unwrap();

        assert_eq!(
            put_response.outcome,
            Outcome::Error {
                msg: BroadcastFailure.to_string()
            }
        );
        assert_eq!(get_response.outcome, Outcome::OfGet { value: None });
    }

    #[tokio::test]
    async fn leader_handles_timed_out_replication() {
        let Runner {
            node_address,
            client,
            ..
        } = setup_with(Role::Leader, vec![]).await;

        let response = client.write_one(&node_address, &PUT_CMD).await.unwrap();

        assert_eq!(
            response.outcome,
            Outcome::Error {
                msg: BroadcastFailure.to_string()
            }
        );
    }

    #[tokio::test]
    async fn leader_handles_get_of_put_value() {
        let outcomes = std::iter::repeat(PUT_OUTCOME.clone())
            .take(*NUM_PEERS)
            .collect::<Vec<Outcome>>();
        let put_command = Command::Put {
            key: "foo".to_string(),
            value: "bar".to_string(),
        };
        let get_command = Command::Get {
            key: "foo".to_string(),
        };
        let Runner {
            node_address,
            client,
            ..
        } = setup_with(Role::Leader, outcomes).await;

        let _ = client.write_one(&node_address, &put_command).await.unwrap();
        let get_response = client.write_one(&node_address, &get_command).await.unwrap();
        assert_eq!(
            get_response.outcome,
            Outcome::OfGet {
                value: Some("bar".to_string())
            }
        );
    }

    #[tokio::test]
    async fn leader_handles_idempotent_puts() {
        let outcomes = std::iter::repeat(PUT_OUTCOME.clone())
            .take(*NUM_PEERS)
            .collect::<Vec<Outcome>>();
        let put_command = Command::Put {
            key: "foo".to_string(),
            value: "bar".to_string(),
        };
        let Runner {
            node_address,
            client,
            ..
        } = setup_with(Role::Leader, outcomes).await;

        let put_response_1 = client.write_one(&node_address, &put_command).await.unwrap();
        let put_response_2 = client.write_one(&node_address, &put_command).await.unwrap();

        assert_eq!(
            put_response_1.outcome,
            Outcome::OfPut { was_modified: true }
        );
        assert_eq!(
            put_response_2.outcome,
            Outcome::OfPut {
                was_modified: false
            }
        );
    }

    #[tokio::test]
    async fn leader_handles_sequential_puts() {
        let outcomes = std::iter::repeat(PUT_OUTCOME.clone())
            .take(*NUM_PEERS)
            .collect::<Vec<Outcome>>();
        let put_command_1 = Command::Put {
            key: "foo".to_string(),
            value: "bar".to_string(),
        };
        let put_command_2 = Command::Put {
            key: "foo".to_string(),
            value: "baz".to_string(),
        };
        let get_command = Command::Get {
            key: "foo".to_string(),
        };

        let Runner {
            node_address,
            client,
            ..
        } = setup_with(Role::Leader, outcomes).await;

        let put_response_1 = client
            .write_one(&node_address, &put_command_1)
            .await
            .unwrap();
        let put_response_2 = client
            .write_one(&node_address, &put_command_2)
            .await
            .unwrap();
        let get_response = client.write_one(&node_address, &get_command).await.unwrap();

        assert_eq!(
            put_response_1.outcome,
            Outcome::OfPut { was_modified: true }
        );
        assert_eq!(
            put_response_2.outcome,
            Outcome::OfPut { was_modified: true }
        );
        assert_eq!(
            get_response.outcome,
            Outcome::OfGet {
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

        let get_response = client.write_one(&node_address, &GET_CMD).await.unwrap();
        assert_eq!(
            get_response.outcome,
            Outcome::Error {
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

        let put_response = client.write_one(&node_address, &PUT_CMD).await.unwrap();
        assert_eq!(put_response.outcome, Outcome::OfPut { was_modified: true });
    }
}
