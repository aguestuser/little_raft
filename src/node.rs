use crate::error::{PermissionError, Result};
use crate::protocol::client::{Client, ClientConfig};
use crate::protocol::request::Command::{Get, Invalid, Set};
use crate::protocol::request::Request;
use crate::protocol::response::Response;
use crate::protocol::server::{Server, ServerConfig};
use crate::store::Store;
use std::net::SocketAddr;
use std::sync::Arc;

pub struct Node {
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
                    Set { key, value } => {
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
        match role.as_ref() {
            Role::Follower => {
                let was_modified = store.set(&key, &value).await;
                Response::of_set(id, was_modified)
            }
            Role::Leader => {
                let set_cmd = Set {
                    key: key.clone(),
                    value: value.clone(),
                };
                match client.broadcast(set_cmd).await {
                    Ok(_) => {
                        let was_modified = store.set(&key, &value).await;
                        Response::of_set(id, was_modified.clone())
                    }
                    Err(e) => Response::error_of(id, e.to_string()),
                }
            }
        }
    }
}

mod test_node {
    // use super::*;

    // #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    // async fn performs_get() {
    //     let (mut writer, mut receiver) = setup().await;
    //
    //     let request: Bytes = [r#"{"id":0,"command":{"type":"Get","key":"foo"}}"#, "\n"]
    //         .concat()
    //         .into();
    //     writer.write_all(&request).await.unwrap();
    //     writer.flush().await.unwrap();
    //
    //     let expected_response: Bytes =
    //         [r#"{"id":0,"outcome":{"type":"OfGet","value":null}}"#, "\n"]
    //             .concat()
    //             .into();
    //     let actual_response = receiver.recv().await.unwrap();
    //     assert_eq!(expected_response, actual_response);
    // }
    //
    // #[tokio::test]
    // async fn performs_set() {
    //     let (mut writer, mut receiver) = setup().await;
    //
    //     let request: Bytes = [
    //         r#"{"id":0,"command":{"type":"Set","key":"foo","value":"bar"}}"#,
    //         "\n",
    //     ]
    //     .concat()
    //     .into();
    //     writer.write_all(&request).await.unwrap();
    //     writer.flush().await.unwrap();
    //
    //     let expected_response: Bytes = [
    //         r#"{"id":0,"outcome":{"type":"OfSet","was_modified":true}}"#,
    //         "\n",
    //     ]
    //     .concat()
    //     .into();
    //     let actual_response = receiver.recv().await.unwrap();
    //     assert_eq!(expected_response, actual_response);
    // }
    //
    // #[tokio::test]
    // async fn performs_set_idempotently() {
    //     let (mut writer, mut receiver) = setup().await;
    //
    //     let req_0: Bytes = [
    //         r#"{"id":0,"command":{"type":"Set","key":"foo","value":"bar"}}"#,
    //         "\n",
    //     ]
    //     .concat()
    //     .into();
    //     let req_1: Bytes = [
    //         r#"{"id":1,"command":{"type":"Set","key":"foo","value":"bar"}}"#,
    //         "\n",
    //     ]
    //     .concat()
    //     .into();
    //
    //     writer.write_all(&req_0).await.unwrap();
    //     writer.flush().await.unwrap();
    //     writer.write_all(&req_1).await.unwrap();
    //     writer.flush().await.unwrap();
    //
    //     let actual_resp_1 = receiver.recv().await.unwrap();
    //     let expected_resp_1: Bytes = [
    //         r#"{"id":0,"outcome":{"type":"OfSet","was_modified":true}}"#,
    //         "\n",
    //     ]
    //     .concat()
    //     .into();
    //     assert_eq!(expected_resp_1, actual_resp_1);
    //
    //     let actual_resp_1 = receiver.recv().await.unwrap();
    //     let expected_resp_1: Bytes = [
    //         r#"{"id":1,"outcome":{"type":"OfSet","was_modified":false}}"#,
    //         "\n",
    //     ]
    //     .concat()
    //     .into();
    //     assert_eq!(expected_resp_1, actual_resp_1);
    // }
    //
    // #[tokio::test]
    // async fn performs_set_and_get() {
    //     let (mut writer, mut receiver) = setup().await;
    //
    //     let get_req_0: Bytes = [r#"{"id":0,"command":{"type":"Get","key":"foo"}}"#, "\n"]
    //         .concat()
    //         .into();
    //     let set_req_1: Bytes = [
    //         r#"{"id":1,"command":{"type":"Set","key":"foo","value":"bar"}}"#,
    //         "\n",
    //     ]
    //     .concat()
    //     .into();
    //     let get_req_2: Bytes = [r#"{"id":2,"command":{"type":"Get","key":"foo"}}"#, "\n"]
    //         .concat()
    //         .into();
    //
    //     writer.write_all(&get_req_0).await.unwrap();
    //     writer.flush().await.unwrap();
    //     writer.write_all(&set_req_1).await.unwrap();
    //     writer.flush().await.unwrap();
    //     writer.write_all(&get_req_2).await.unwrap();
    //     writer.flush().await.unwrap();
    //
    //     let expected_resp_0: Bytes = [r#"{"id":0,"outcome":{"type":"OfGet","value":null}}"#, "\n"]
    //         .concat()
    //         .into();
    //     let actual_resp_0 = receiver.recv().await.unwrap();
    //     assert_eq!(expected_resp_0, actual_resp_0);
    //
    //     let expected_resp_1: Bytes = [
    //         r#"{"id":1,"outcome":{"type":"OfSet","was_modified":true}}"#,
    //         "\n",
    //     ]
    //     .concat()
    //     .into();
    //     let actual_resp_1 = receiver.recv().await.unwrap();
    //     assert_eq!(expected_resp_1, actual_resp_1);
    //
    //     let expected_resp_2: Bytes = [r#"{"id":2,"outcome":{"type":"OfGet","value":"bar"}}"#, "\n"]
    //         .concat()
    //         .into();
    //     let actual_resp_2 = receiver.recv().await.unwrap();
    //     assert_eq!(expected_resp_2, actual_resp_2);
    // }
    //
    // #[tokio::test]
    // async fn handles_invalid_request() {
    //     let (mut writer, mut receiver) = setup().await;
    //
    //     let req: Bytes = [r#"{"id":0,"command":{"type":"Set","key":"foo"}}"#, "\n"]
    //         .concat()
    //         .into();
    //
    //     writer.write_all(&req).await.unwrap();
    //     writer.flush().await.unwrap();
    //
    //     let actual_resp = receiver.recv().await.unwrap();
    //     let expected_resp: Bytes = [
    //         r#"{"id":12459724080765958563,"outcome":{"type":"Error","msg":"missing field `value` at line 1 column 45"}}"#,
    //         "\n",
    //     ]
    //     .concat()
    //     .into();
    //     assert_eq!(expected_resp, actual_resp);
    // }
}
