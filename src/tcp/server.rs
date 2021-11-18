use std::net::SocketAddr;
use std::sync::Arc;

use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::oneshot::Sender as OneShotSender;
use tokio::sync::{mpsc, oneshot, Mutex};

use crate::error::Result;
use crate::tcp::connection::ServerConnection;
use crate::tcp::request::Request;
use crate::tcp::response::Response;
use crate::tcp::REQUEST_BUFFER_SIZE;

pub struct Server {
    address: SocketAddr,
    tcp_listener: Option<Arc<TcpListener>>,
    request_sender: Sender<(Request, Responder)>,
    pub request_receiver: Arc<Mutex<Receiver<(Request, Responder)>>>,
}

#[derive(Clone)]
pub struct ServerConfig {
    pub address: SocketAddr,
}

type Responder = OneShotSender<Response>;

impl Server {
    pub fn new(cfg: ServerConfig) -> Server {
        let (request_sender, request_receiver) =
            mpsc::channel::<(Request, Responder)>(REQUEST_BUFFER_SIZE);
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
        request_tx: Sender<(Request, OneShotSender<Response>)>,
    ) {
        let connection = Arc::new(ServerConnection::new(socket));

        tokio::spawn(async move {
            loop {
                let (response_tx, response_rx) = oneshot::channel::<Response>();

                match connection.read().await {
                    Ok(req) => {
                        println!("req: {:?}", req);
                        let _ = request_tx.send((req, response_tx)).await;
                    }
                    Err(e) => {
                        println!("err");
                        // TODO: bubble up?
                        eprintln!("ERROR reading request: {}", e.to_string());
                    }
                }

                let write_connection = connection.clone();
                tokio::spawn(async move {
                    // TODO: insert timeout here
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

    use crate::tcp::NEWLINE;
    use crate::test_support::gen::Gen;

    use super::*;

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

        let request_bytes: Vec<u8> = Gen::request().into();
        let client_write = [request_bytes.clone().as_slice(), b"\n"].concat();

        client_writer.write_all(&*client_write).await.unwrap();
        client_writer.flush().await.unwrap();

        let expected_request: Request = request_bytes.into();
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

        let request_bytes: Vec<u8> = Gen::request().into();
        let client_write = [request_bytes.clone().as_slice(), b"\n"].concat();

        client_writer.write_all(&*client_write).await.unwrap();
        client_writer.flush().await.unwrap();

        let response = Gen::response();
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
