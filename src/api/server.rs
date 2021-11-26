use std::net::SocketAddr;
use std::sync::Arc;

use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::Sender;
use tokio::sync::oneshot;
use tokio::sync::oneshot::Sender as OneShotSender;

use crate::api::request::ApiRequestEnvelope;
use crate::api::response::{ApiResponse, ApiResponseEnvelope};
use crate::api::ApiServerConnection;
use crate::error::Result;

pub type RespondableApiRequest = (ApiRequestEnvelope, ApiResponder);
pub type ApiResponder = OneShotSender<ApiResponseEnvelope>;

pub struct ApiServerConfig {
    pub address: SocketAddr,
}
pub struct ApiServer {
    pub address: SocketAddr, // TODO: use this to issue `stop()`
}

impl ApiServerConfig {
    pub async fn run_with(self, request_tx: Sender<RespondableApiRequest>) -> Result<ApiServer> {
        let tcp_listener = TcpListener::bind(self.address.clone()).await?;
        println!("> ApiServer listening on {:?}", &self.address);

        tokio::spawn(async move {
            // TODO: use select loop here to handle poison pill for shutdown
            loop {
                let (socket, client_addr) = tcp_listener.accept().await.unwrap();
                println!("> ApiServer got connection on {}", &client_addr);
                let request_tx = request_tx.clone();
                tokio::spawn(async move { ApiServer::handle_messages(socket, request_tx).await });
            }
        });

        Ok(ApiServer {
            address: self.address,
        })
    }
}

impl ApiServer {
    /// Process incoming requests on a `socket`, emit them in a tuple along with a one-shot responder
    /// over a `request_tx` to a subscriber (to whom we delegate the business logic of determining
    /// how to respond), then issue whatever `ApiResponse` is received from the responder back to
    /// the `ApiClient` from whom we received the request.
    async fn handle_messages(socket: TcpStream, request_tx: Sender<RespondableApiRequest>) {
        let connection = Arc::new(ApiServerConnection::new(socket));

        tokio::spawn(async move {
            loop {
                let (response_tx, response_rx) = oneshot::channel::<ApiResponseEnvelope>();

                match connection.read().await {
                    Ok(req) => {
                        let _ = request_tx.send((req, response_tx)).await;
                    }
                    Err(e) => {
                        let _ = response_tx.send(ApiResponseEnvelope {
                            id: 0,
                            response: ApiResponse::ServerError { msg: e.to_string() },
                        });
                    }
                }

                let write_connection = connection.clone();
                tokio::spawn(async move {
                    // TODO: insert timeout here?
                    if let Ok(response) = response_rx.await {
                        let _ = write_connection.write(response).await;
                    }
                });
            }
        });
    }

    // async fn stop(&self) {
    // TODO: shut down gracefully
    //  - strategy: pass a poison pill to loop spawned in `run()` (which should become a select)!
    //  - leverages cancel safety: https://docs.rs/tokio/1.12.0/tokio/net/struct.TcpListener.html#cancel-safety
    //  - see: https://docs.rs/tokio/1.12.0/tokio/macro.select.html
    //  - and: https://stackoverflow.com/questions/48334079/is-it-possible-to-close-a-tcplistener-in-tokio
    //}
}

#[cfg(test)]
mod api_server_tests {
    use test_context::{test_context, AsyncTestContext};
    use tokio::sync::mpsc;
    use tokio::sync::mpsc::Receiver;

    use crate::api::ApiClientConnection;
    use crate::test_support::gen::Gen;
    use crate::CHAN_BUF_SIZE;

    use super::*;

    lazy_static! {
        static ref BUF_SIZE: usize = 10;
    }

    struct RunningServer {
        request_rx: Receiver<RespondableApiRequest>,
        client_conn: ApiClientConnection,
    }

    #[async_trait::async_trait]
    impl AsyncTestContext for RunningServer {
        async fn setup() -> Self {
            let address = Gen::socket_addr();
            let (request_tx, request_rx) = mpsc::channel::<RespondableApiRequest>(CHAN_BUF_SIZE);

            // TODO: hold onto this assignment to test shutdown...
            let _ = ApiServerConfig { address }
                .run_with(request_tx)
                .await
                .unwrap();

            let socket = TcpStream::connect(address).await.unwrap();
            let client_conn = ApiClientConnection::new(socket);

            Self {
                request_rx,
                client_conn,
            }
        }
    }

    #[test_context(RunningServer)]
    #[tokio::test]
    async fn listens_for_requests_from_client_and_puts_them_on_channel(mut ctx: RunningServer) {
        let expected_req = Gen::api_request_envelope();
        let _ = ctx.client_conn.write(expected_req.clone()).await.unwrap();
        let (actual_req, _) = ctx.request_rx.recv().await.unwrap();
        assert_eq!(expected_req, actual_req);
    }

    #[test_context(RunningServer)]
    #[tokio::test]
    async fn listens_for_responses_on_channel_and_writes_them_to_client(mut ctx: RunningServer) {
        let request = Gen::api_request_envelope();
        let _ = ctx.client_conn.write(request.clone()).await.unwrap();

        let expected_response = Gen::api_response_envelope();
        let (_, responder) = ctx.request_rx.recv().await.unwrap();
        let _ = responder.send(expected_response.clone()).unwrap();

        let actual_response = ctx.client_conn.read().await.unwrap();
        assert_eq!(expected_response, actual_response);
    }
}
