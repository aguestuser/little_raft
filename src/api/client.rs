use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use dashmap::DashMap;
use tokio::net::TcpStream;
use tokio::sync::oneshot;
use tokio::sync::oneshot::Sender as OneShotSender;
use tokio::time;
use tokio::time::Duration;

use crate::api::request::{ApiRequest, ApiRequestEnvelope};
use crate::api::response::{ApiResponse, ApiResponseEnvelope};
use crate::api::ApiClientConnection;
use crate::error::NetworkError::{ConnectionClosed, RequestTimeout};
use crate::error::ProtocolError::{BadResponse, LeaderRequired, ServerError};
use crate::error::{AsyncError, Result};

#[cfg(not(test))]
const TIMEOUT_IN_MILLIS: u64 = 2000;
#[cfg(test)]
const TIMEOUT_IN_MILLIS: u64 = 80;

pub struct ApiClient {
    server_address: SocketAddr,
    connection: Option<Arc<ApiClientConnection>>,
    response_handlers: Arc<DashMap<u64, OneShotSender<ApiResponseEnvelope>>>,
    request_id: AtomicU64,
}

#[derive(Clone)]
pub struct ApiClientConfig {
    pub server_address: SocketAddr,
}

impl ApiClient {
    /// Construct a `Client` from a `Client` config, leaving "live" resources to be initialized
    /// later in `Client::run`.
    pub fn new(cfg: ApiClientConfig) -> ApiClient {
        let ApiClientConfig { server_address } = cfg;
        Self {
            server_address,
            connection: None,
            response_handlers: Arc::new(DashMap::new()),
            request_id: AtomicU64::new(0),
        }
    }

    /// Atomically fetch and increment an id for request tagging (this enables us to tell
    /// which responses correspond to which requests while enabling the same underlying
    /// request to be issued to multiple peers, each with a different id).
    pub fn next_id(&self) -> u64 {
        self.request_id.fetch_add(1, Ordering::SeqCst)
    }

    /// Create TCP socket connections to all peers, then store a reference to each connection, and
    /// listen for responses on it, forwarding any responses to one-shot-receiver handlers registered
    /// in `Client::write`  and removing the handlers from the handler registry once they are used.
    pub async fn run(&mut self) -> Result<()> {
        // open tcp socket connection to server
        let stream = TcpStream::connect(self.server_address).await?;
        let connection = Arc::new(ApiClientConnection::new(stream));
        self.connection = Some(connection.clone());
        let handlers = self.response_handlers.clone();

        // listen for responses on socket and pass them to response handlers
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

        Ok(())
    }

    pub async fn get(&self, key: &str) -> Result<Option<String>> {
        let request = ApiRequestEnvelope {
            id: self.next_id(),
            body: ApiRequest::Get {
                key: key.to_string(),
            },
        };
        let response = self.write(request).await?;
        match response.body {
            ApiResponse::ToGet { value } => Ok(value),
            ApiResponse::ServerError { msg } => Err(ServerError(msg).boxed()),
            _ => Err(BadResponse(response.body.display_type()).boxed()),
        }
    }

    pub async fn put(&self, key: &str, value: &str) -> Result<bool> {
        let request = ApiRequestEnvelope {
            id: self.next_id(),
            body: ApiRequest::Put {
                key: key.to_string(),
                value: value.to_string(),
            },
        };
        let response: ApiResponseEnvelope = self.write(request).await?;
        match response.body {
            ApiResponse::ToPut { was_modified } => Ok(was_modified),
            ApiResponse::ServerError { msg } => Err(ServerError(msg).boxed()),
            ApiResponse::Redirect { leader_address } => Err(LeaderRequired(leader_address).boxed()),
            _ => Err(BadResponse(response.body.display_type()).boxed()),
        }
    }

    /// Write a `request` to a peer `connection` and register a one-shot sender to
    /// handle the peer's response in the shared `response_handlers` hash map owned by the `Client`.
    /// Then wait to either receive the response and return an `Ok<Response>` or timeout
    /// and return an `Err`.
    async fn write(&self, request: ApiRequestEnvelope) -> Result<ApiResponseEnvelope> {
        let connection = self
            .connection
            .as_ref()
            .ok_or_else(|| Box::new(ConnectionClosed) as AsyncError)?
            .clone();
        let handlers = self.response_handlers.clone();

        let (response_tx, response_rx) = oneshot::channel::<ApiResponseEnvelope>();
        let _ = handlers.insert(request.id, response_tx);
        connection.write(request).await?;

        return tokio::select! {
            response = response_rx => {
                response.map_err(|_| Box::new(ConnectionClosed) as AsyncError)
            }
            _ = time::sleep(Duration::from_millis(TIMEOUT_IN_MILLIS)) => {
                Err(Box::new(RequestTimeout))
            }
        };
    }
}

/*********
 * TESTS *
 *********/

#[cfg(test)]
mod test_client {
    use tokio::net::TcpListener;
    use tokio::sync::mpsc;
    use tokio::sync::mpsc::Receiver;

    use crate::api::ApiServerConnection;
    use crate::test_support::gen::Gen;

    use super::*;

    struct Runner {
        client_config: ApiClientConfig,
        req_rx: Receiver<ApiRequestEnvelope>,
    }

    lazy_static! {
        static ref NUM_PEERS: usize = 5;
        static ref MAJORITY: usize = *NUM_PEERS / 2;
        static ref PUT_REQ: ApiRequest = ApiRequest::Put {
            key: "foo".to_string(),
            value: "bar".to_string(),
        };
    }

    async fn setup() -> Runner {
        setup_with(None, None).await
    }

    async fn setup_with_response(response: ApiResponse) -> Runner {
        setup_with(Some(response), None).await
    }

    async fn setup_with(response: Option<ApiResponse>, fuzzed_id: Option<u64>) -> Runner {
        let buf_size = 1;
        let server_address = Gen::socket_addr();
        let (req_tx, req_rx) = mpsc::channel::<ApiRequestEnvelope>(buf_size);
        let listener = TcpListener::bind(server_address.clone()).await.unwrap();
        // println!("> Test server listening at {:?}", server_address);

        tokio::spawn(async move {
            let (socket, _client_addr) = listener.accept().await.unwrap();
            // println!("> Test server received connection from {:?}", _client_addr);
            let conn = ApiServerConnection::new(socket);

            // TODO: put the below in a loop if we want to test multiple requests/responses
            let req = conn.read().await.unwrap();
            // println!("> Test server got request: {:?}", req);
            // report receipt of request to test harness receiver
            req_tx.send(req.clone()).await.unwrap();
            // send canned response provided by test harness to client
            if let Some(body) = response {
                let env = ApiResponseEnvelope {
                    id: fuzzed_id.unwrap_or(req.id),
                    body,
                };
                conn.write(env).await.unwrap()
            };
        });

        Runner {
            client_config: ApiClientConfig { server_address },
            req_rx,
        }
    }

    #[tokio::test]
    async fn constructs_a_client() {
        let cfg = ApiClientConfig {
            server_address: Gen::socket_addr(),
        };
        let client = ApiClient::new(cfg.clone());
        assert_eq!(client.server_address, cfg.server_address.clone());
    }

    #[tokio::test]
    async fn provides_incrementing_ids() {
        let client = ApiClient::new(Gen::api_client_config());

        assert_eq!(client.next_id(), 0);
        assert_eq!(client.next_id(), 1);
    }

    #[tokio::test]
    async fn connects_to_server() {
        let Runner { client_config, .. } = setup().await;

        let mut client = ApiClient::new(client_config);
        let _ = client.run().await.unwrap();

        //TODO: assert that server address is in use
    }

    #[tokio::test]
    async fn performs_get_request() {
        let expected_request = ApiRequest::Get {
            key: "foo".to_string(),
        };
        let expected_response = Some("bar".to_string());

        let Runner {
            client_config,
            mut req_rx,
        } = setup_with_response(ApiResponse::ToGet {
            value: expected_response.clone(),
        })
        .await;

        let mut client = ApiClient::new(client_config);
        let _ = client.run().await.unwrap();

        let actual_response = client.get("foo").await.unwrap();
        let actual_request = req_rx.recv().await.unwrap().body;

        assert_eq!(actual_request, expected_request);
        assert_eq!(actual_response, expected_response);
    }

    #[tokio::test]
    async fn performs_put_request() {
        let expected_request = ApiRequest::Put {
            key: "foo".to_string(),
            value: "bar".to_string(),
        };
        let expected_response = true;

        let Runner {
            client_config,
            mut req_rx,
        } = setup_with_response(ApiResponse::ToPut {
            was_modified: expected_response.clone(),
        })
        .await;

        let mut client = ApiClient::new(client_config);
        let _ = client.run().await.unwrap();

        let actual_response = client.put("foo", "bar").await.unwrap();
        let actual_request = req_rx.recv().await.unwrap().body;

        assert_eq!(actual_request, expected_request);
        assert_eq!(actual_response, expected_response);
    }

    #[tokio::test]
    async fn handles_timeout() {
        let Runner {
            client_config,
            mut req_rx,
        } = setup_with(Some(Gen::api_response()), Some(Gen::u64())).await;

        let mut client = ApiClient::new(client_config);
        client.run().await.unwrap();

        let resp = client.get("foo").await;
        let _ = req_rx.recv().await.unwrap();

        assert!(resp.is_err());
        assert_eq!(resp.err().unwrap().to_string(), RequestTimeout.to_string());
    }
}
