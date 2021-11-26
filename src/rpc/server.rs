use std::net::SocketAddr;
use std::sync::Arc;

use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::Sender;
use tokio::sync::oneshot;
use tokio::sync::oneshot::Sender as OneShotSender;

use crate::error::Result;
use crate::rpc::request::RpcRequestEnvelope;
use crate::rpc::response::RpcResponseEnvelope;
use crate::rpc::RpcServerConnection;

pub type RespondableRpcRequest = (RpcRequestEnvelope, RpcResponder);

pub type RpcResponder = OneShotSender<RpcResponseEnvelope>;

pub struct RpcServerConfig {
    pub address: SocketAddr,
}

#[allow(unused)]
pub struct RpcServer {
    address: SocketAddr, // TODO: use to issue `stop()`
}

impl RpcServerConfig {
    pub async fn run_with(self, request_tx: Sender<RespondableRpcRequest>) -> Result<RpcServer> {
        let tcp_listener = TcpListener::bind(&self.address).await?;
        println!("> RpcServer listening on {:?}", &self.address);

        tokio::spawn(async move {
            // TODO: use select here to insert kill switch for shutdown
            loop {
                let (socket, client_addr) = tcp_listener.accept().await.unwrap();
                println!("> RpcServer got connection on {}", &client_addr);
                let request_tx = request_tx.clone();
                tokio::spawn(async move { RpcServer::handle_messages(socket, request_tx).await });
            }
        });

        Ok(RpcServer {
            address: self.address,
        })
    }
}

impl RpcServer {
    /// Process data from a socket connection
    async fn handle_messages(
        socket: TcpStream,
        request_tx: Sender<(RpcRequestEnvelope, OneShotSender<RpcResponseEnvelope>)>,
    ) {
        let connection = Arc::new(RpcServerConnection::new(socket));
        tokio::spawn(async move {
            loop {
                if let Ok(req) = connection.read().await {
                    let (response_tx, response_rx) = oneshot::channel::<RpcResponseEnvelope>();
                    let _ = request_tx.send((req, response_tx)).await;
                    let write_connection = connection.clone();
                    tokio::spawn(async move {
                        // TODO: insert timeout here?
                        if let Ok(response) = response_rx.await {
                            let _ = write_connection.write(response).await;
                        }
                    });
                }
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
mod rpc_server_tests {
    use test_context::{test_context, AsyncTestContext};
    use tokio::sync::mpsc;
    use tokio::sync::mpsc::Receiver;

    use crate::test_support::gen::Gen;
    use crate::CHAN_BUF_SIZE;

    use super::*;
    use crate::rpc::RpcClientConnection;

    lazy_static! {
        static ref BUF_SIZE: usize = 10;
    }

    struct RunningServer {
        request_rx: Receiver<RespondableRpcRequest>,
        client_conn: RpcClientConnection,
    }

    #[async_trait::async_trait]
    impl AsyncTestContext for RunningServer {
        async fn setup() -> Self {
            let address = Gen::socket_addr();
            let (request_tx, request_rx) = mpsc::channel::<RespondableRpcRequest>(CHAN_BUF_SIZE);

            // TODO: hold onto this assignment to test shutdown...
            let _ = RpcServerConfig { address }
                .run_with(request_tx)
                .await
                .unwrap();

            let socket = TcpStream::connect(address).await.unwrap();
            let client_conn = RpcClientConnection::new(socket);

            Self {
                request_rx,
                client_conn,
            }
        }
    }

    #[test_context(RunningServer)]
    #[tokio::test]
    async fn listens_for_requests_from_client_and_puts_them_on_channel(mut ctx: RunningServer) {
        let expected_req = Gen::rpc_request_envelope();
        let _ = ctx.client_conn.write(expected_req.clone()).await.unwrap();
        let (actual_req, _) = ctx.request_rx.recv().await.unwrap();
        assert_eq!(expected_req, actual_req);
    }

    #[test_context(RunningServer)]
    #[tokio::test]
    async fn listens_for_responses_on_channel_and_writes_them_to_client(mut ctx: RunningServer) {
        let request = Gen::rpc_request_envelope();
        let _ = ctx.client_conn.write(request.clone()).await.unwrap();

        let expected_response = Gen::rpc_response_envelope();
        let (_, responder) = ctx.request_rx.recv().await.unwrap();
        let _ = responder.send(expected_response.clone()).unwrap();

        let actual_response = ctx.client_conn.read().await.unwrap();
        assert_eq!(expected_response, actual_response);
    }
}
