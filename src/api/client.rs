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

type ApiCallbackRegistry = Arc<DashMap<u64, OneShotSender<ApiResponseEnvelope>>>;

#[derive(Clone)]
pub struct ApiClientConfig {
    pub server_address: SocketAddr,
}

pub struct ApiClient {
    connection: Arc<ApiClientConnection>,
    on_response_callbacks: ApiCallbackRegistry,
    request_id: AtomicU64,
}

impl ApiClientConfig {
    /// Create a live `ApiClient` from an inert `ApiClientConfig` as follows: Create TCP socket
    /// connections to all peers, then store a reference to each connection, and listen for
    /// responses on it, forwarding any responses to callbacks registered in `Client::write`,
    /// removing the handlers from the handler registry as they are used.
    pub async fn run(self) -> Result<ApiClient> {
        // open tcp socket connection to server
        let connection = Arc::new(ApiClientConnection::new(
            TcpStream::connect(self.server_address).await?,
        ));

        // construct machinery for matching responses to requests
        let request_id = AtomicU64::new(0);
        let on_response_callbacks: ApiCallbackRegistry = Arc::new(DashMap::new());

        // listen for responses on socket and pass them to response handlers
        let callbacks = on_response_callbacks.clone();
        let conn = connection.clone();
        tokio::spawn(async move {
            loop {
                if let Ok(response) = conn.read().await {
                    // send the responses over a oneshot channel to handlers registered in #write (below)
                    if let Some((_, callback)) = callbacks.remove(&response.id) {
                        let _ = callback.send(response);
                    }
                }
            }
        });

        // Return live client to caller
        Ok(ApiClient {
            connection,
            on_response_callbacks,
            request_id,
        })
    }
}

impl ApiClient {
    /// Atomically fetch and increment an id for request tagging (this enables us to tell
    /// which responses correspond to which requests while enabling the same underlying
    /// request to be issued to multiple peers, each with a different id).
    pub fn next_id(&self) -> u64 {
        self.request_id.fetch_add(1, Ordering::SeqCst)
    }

    // TODO: legacy
    pub fn run() -> Result<()> {
        Ok(())
    }

    pub async fn get(&self, key: &str) -> Result<Option<String>> {
        let request = ApiRequestEnvelope {
            id: self.next_id(),
            request: ApiRequest::Get {
                key: key.to_string(),
            },
        };
        let response = self.write(request).await?;
        match response.response {
            ApiResponse::ToGet { value } => Ok(value),
            ApiResponse::ServerError { msg } => Err(ServerError(msg).boxed()),
            _ => Err(BadResponse(response.response.display_type()).boxed()),
        }
    }

    pub async fn put(&self, key: &str, value: &str) -> Result<bool> {
        let request = ApiRequestEnvelope {
            id: self.next_id(),
            request: ApiRequest::Put {
                key: key.to_string(),
                value: value.to_string(),
            },
        };
        let response: ApiResponseEnvelope = self.write(request).await?;
        match response.response {
            ApiResponse::ToPut { was_modified } => Ok(was_modified),
            ApiResponse::ServerError { msg } => Err(ServerError(msg).boxed()),
            ApiResponse::Redirect { leader_address } => Err(LeaderRequired(leader_address).boxed()),
            _ => Err(BadResponse(response.response.display_type()).boxed()),
        }
    }

    /// Write a `request` to a peer `connection` and register a one-shot sender to
    /// handle the peer's response in the shared `response_handlers` hash map owned by the `Client`.
    /// Then wait to either receive the response and return an `Ok<Response>` or timeout
    /// and return an `Err`.
    async fn write(&self, request: ApiRequestEnvelope) -> Result<ApiResponseEnvelope> {
        let handlers = self.on_response_callbacks.clone();
        let (response_tx, response_rx) = oneshot::channel::<ApiResponseEnvelope>();
        let _ = handlers.insert(request.id, response_tx);
        self.connection.write(request).await?;

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
mod test_api_client {
    use test_context::{test_context, AsyncTestContext};
    use tokio::net::TcpListener;
    use tokio::sync::mpsc;
    use tokio::sync::mpsc::Receiver;

    use crate::api::ApiServerConnection;
    use crate::test_support::gen::Gen;

    use super::*;

    lazy_static! {
        static ref NUM_PEERS: usize = 5;
        static ref GET_REQUEST: ApiRequest = ApiRequest::Get {
            key: "foo".to_string(),
        };
        static ref PUT_REQUEST: ApiRequest = ApiRequest::Put {
            key: "foo".to_string(),
            value: "bar".to_string(),
        };
        static ref GET_RESPONSE: ApiResponse = ApiResponse::ToGet {
            value: Some("bar".to_string()),
        };
        static ref PUT_RESPONSE: ApiResponse = ApiResponse::ToPut { was_modified: true };
    }

    struct Context {
        client: ApiClient,
        request_rx: Receiver<ApiRequestEnvelope>,
    }

    impl Context {
        async fn setup(response: Option<ApiResponse>, fuzzed_id: Option<u64>) -> Self {
            let buf_size = 1;
            let server_address = Gen::socket_addr();
            let (req_tx, request_rx) = mpsc::channel::<ApiRequestEnvelope>(buf_size);
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
                        response: body,
                    };
                    let _ = conn.write(env).await.unwrap();
                    let _ = conn.close().await.unwrap();
                };
            });

            Self {
                client: ApiClientConfig { server_address }.run().await.unwrap(),
                request_rx,
            }
        }
        // TODO: teardown by calling client.stop()
    }

    struct ClientReceivingGetResponse(Context);
    #[async_trait::async_trait]
    impl AsyncTestContext for ClientReceivingGetResponse {
        async fn setup() -> Self {
            let ctx = Context::setup(Some(GET_RESPONSE.clone()), None).await;
            Self(ctx)
        }
    }

    struct ClientReceivingPutResponse(Context);
    #[async_trait::async_trait]
    impl AsyncTestContext for ClientReceivingPutResponse {
        async fn setup() -> Self {
            let ctx = Context::setup(Some(PUT_RESPONSE.clone()), None).await;
            Self(ctx)
        }
    }

    struct ClientReceivingTimeout(Context);
    #[async_trait::async_trait]
    impl AsyncTestContext for ClientReceivingTimeout {
        async fn setup() -> Self {
            let ctx = Context::setup(Some(Gen::api_response()), Some(Gen::u64())).await;
            Self(ctx)
        }
    }

    #[test_context(ClientReceivingGetResponse)]
    #[tokio::test]
    async fn performs_get_request(mut ctx: ClientReceivingGetResponse) {
        let actual_response = ctx.0.client.get("foo").await.unwrap();
        let actual_request = ctx.0.request_rx.recv().await.unwrap().request;

        assert_eq!(actual_request, GET_REQUEST.clone());
        assert_eq!(actual_response, Some("bar".to_string()));
    }

    #[test_context(ClientReceivingPutResponse)]
    #[tokio::test]
    async fn performs_put_request(mut ctx: ClientReceivingPutResponse) {
        let expected_request = PUT_REQUEST.clone();
        let expected_response = true;

        let actual_response = ctx.0.client.put("foo", "bar").await.unwrap();
        let actual_request = ctx.0.request_rx.recv().await.unwrap().request;

        assert_eq!(actual_request, expected_request);
        assert_eq!(actual_response, expected_response);
    }

    #[test_context(ClientReceivingTimeout)]
    #[tokio::test]
    async fn handles_timeout(mut ctx: ClientReceivingTimeout) {
        let result = ctx.0.client.get("foo").await;
        let _ = ctx.0.request_rx.recv().await.unwrap();

        assert!(result.is_err());
        assert_eq!(result.err().unwrap().downcast_ref(), Some(&RequestTimeout));
    }
}
