use std::net::SocketAddr;
use std::sync::Arc;

use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::oneshot::Sender as OneShotSender;
use tokio::sync::{mpsc, oneshot, Mutex};

use crate::api::request::RequestEnvelope;
use crate::api::response::{Response, ResponseEnvelope};
use crate::api::ServerConnection;
use crate::api::REQUEST_BUFFER_SIZE;
use crate::Result;

pub struct Server {
    address: SocketAddr,
    tcp_listener: Option<Arc<TcpListener>>,
    request_sender: Sender<(RequestEnvelope, Responder)>,
    pub request_receiver: Arc<Mutex<Receiver<(RequestEnvelope, Responder)>>>,
}

#[derive(Clone)]
pub struct ServerConfig {
    pub address: SocketAddr,
}

type Responder = OneShotSender<ResponseEnvelope>;

impl Server {
    pub fn new(cfg: ServerConfig) -> Server {
        let (request_sender, request_receiver) =
            mpsc::channel::<(RequestEnvelope, Responder)>(REQUEST_BUFFER_SIZE);
        Self {
            address: cfg.address,
            tcp_listener: Option::None,
            request_sender,
            request_receiver: Arc::new(Mutex::new(request_receiver)),
        }
    }

    pub async fn run(&mut self) -> Result<()> {
        let tcp_listener_arc = Arc::new(TcpListener::bind(&self.address).await.unwrap());
        let tcp_listener = tcp_listener_arc.clone();
        self.tcp_listener = Some(tcp_listener_arc);
        println!("> Listening on {:?}", &self.address);

        let request_sender = self.request_sender.clone();
        tokio::spawn(async move {
            // TODO: use select here to insert kill switch for shutdown
            loop {
                let (socket, client_addr) = tcp_listener.accept().await.unwrap();
                println!("> Got connection on {}", &client_addr);
                let request_sender = request_sender.clone();
                tokio::spawn(async move { Server::handle_messages(socket, request_sender).await });
            }
        });

        Ok(())
    }

    // async fn stop(&self) {
    // TODO: shut down gracefully
    //  - strategy: pass a poison pill to main loop (which should become a select)!
    //  - leverages cancel safety: https://docs.rs/tokio/1.12.0/tokio/net/struct.TcpListener.html#cancel-safety
    //  - see: https://docs.rs/tokio/1.12.0/tokio/macro.select.html
    //  - and: https://stackoverflow.com/questions/48334079/is-it-possible-to-close-a-tcplistener-in-tokio
    //}

    /// Process data from a socket connection
    async fn handle_messages(
        socket: TcpStream,
        request_tx: Sender<(RequestEnvelope, OneShotSender<ResponseEnvelope>)>,
    ) {
        let connection = Arc::new(ServerConnection::new(socket));

        tokio::spawn(async move {
            loop {
                let (response_tx, response_rx) = oneshot::channel::<ResponseEnvelope>();

                match connection.read().await {
                    Ok(req) => {
                        let _ = request_tx.send((req, response_tx)).await;
                    }
                    Err(e) => {
                        eprintln!("ERROR reading request: {}", e.to_string());
                        let _ = response_tx.send(ResponseEnvelope {
                            id: u64::MAX,
                            body: Response::Error { msg: e.to_string() },
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
}

#[cfg(test)]
mod server_tests {
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter};
    use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};

    use crate::test_support::gen::Gen;
    use crate::NEWLINE;

    use super::*;
    use std::prelude::rust_2021::TryInto;

    lazy_static! {
        static ref BUF_SIZE: usize = 10;
    }

    struct Runner {
        server: Server,
        client_reader: BufReader<OwnedReadHalf>,
        client_writer: BufWriter<OwnedWriteHalf>,
    }

    async fn setup() -> Runner {
        let address = Gen::socket_addr();
        // TODO: return this when testing Server#stop()
        let mut server = Server::new(ServerConfig { address });
        let _ = server.run().await;

        let (client_reader, client_writer) =
            match TcpStream::connect(address).await.unwrap().into_split() {
                (r, w) => (BufReader::new(r), BufWriter::new(w)),
            };

        Runner {
            server,
            client_reader,
            client_writer,
        }
    }

    #[tokio::test]
    async fn constructs_server_struct() {
        let config = Gen::server_config();
        let server = Server::new(config.clone());
        assert_eq!(server.address, config.address);
        assert!(server.tcp_listener.is_none());
    }

    #[tokio::test]
    async fn listens_for_requests_from_client_and_puts_them_on_channel() {
        let Runner {
            server,
            mut client_writer,
            ..
        } = setup().await;

        let request_bytes: Vec<u8> = Gen::request_envelope().into();
        let client_write = [request_bytes.clone().as_slice(), b"\n"].concat();

        client_writer.write_all(&*client_write).await.unwrap();
        client_writer.flush().await.unwrap();

        let expected_request: RequestEnvelope = request_bytes.try_into().unwrap();
        let (actual_request, _) = server.request_receiver.lock().await.recv().await.unwrap();
        assert_eq!(expected_request, actual_request);
    }

    #[tokio::test]
    async fn listens_for_responses_on_channel_and_writes_them_to_client() {
        let Runner {
            server,
            mut client_reader,
            mut client_writer,
        } = setup().await;

        let request_bytes: Vec<u8> = Gen::request_envelope().into();
        let client_write = [request_bytes.clone().as_slice(), b"\n"].concat();

        client_writer.write_all(&*client_write).await.unwrap();
        client_writer.flush().await.unwrap();

        let response = Gen::response_envelope();
        let response_bytes: Vec<u8> = response.clone().into();
        let (_, responder) = server.request_receiver.lock().await.recv().await.unwrap();
        let _ = responder.send(response).unwrap();

        let expected_client_read = [response_bytes, vec![NEWLINE]].concat();
        let mut actual_client_read = Vec::<u8>::new();
        let _ = client_reader
            .read_until(NEWLINE, &mut actual_client_read)
            .await
            .unwrap();

        assert_eq!(expected_client_read, actual_client_read);
    }
}
