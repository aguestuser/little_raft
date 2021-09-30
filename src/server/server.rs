use std::sync::Arc;

use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;

use crate::frame::request::Request;
use crate::frame::response::Response;
use crate::server::config::Config;
use crate::server::connection::Connection;
use crate::server::store::Store;

pub struct Server {/*store: Arc<Store>, tcp_listener: Arc<TcpListener>*/}

impl Server {
    pub async fn run(cfg: Config) -> JoinHandle<()> {
        let store_arc = Arc::new(Store::new());
        let store = store_arc.clone();
        println!("> Created store");

        let addr = cfg.addr;
        let sock_srv_arc = Arc::new(TcpListener::bind(addr).await.unwrap());
        let sock_srv = sock_srv_arc.clone();
        println!("> Listening on {:?}", &addr);

        tokio::spawn(async move {
            // TODO: use select here to insert kill switch for shutdown
            loop {
                let (socket, client_addr) = sock_srv.accept().await.unwrap();
                println!("> Got connection on {}", &client_addr);

                let store = store.clone();
                tokio::spawn(async move { Server::handle_requests(socket, store).await });
            }
        })
    }

    // async fn stop(&self) {
    // TODO: shut down gracefully
    //  - strategy: pass a poison pill to main loop (which should become a select)!
    //  - leverages cancel safety: https://docs.rs/tokio/1.12.0/tokio/net/struct.TcpListener.html#cancel-safety
    //  - see: https://docs.rs/tokio/1.12.0/tokio/macro.select.html
    //  - and: https://stackoverflow.com/questions/48334079/is-it-possible-to-close-a-tcplistener-in-tokio
    //}

    /// Process data from a socket connection
    async fn handle_requests(socket: TcpStream, store: Arc<Store>) {
        let mut connection = Connection::new(socket);

        loop {
            match connection.read().await {
                Ok(req) => {
                    let response: Response = match req {
                        Request::Get { id, key } => {
                            let value = store.get(&key).await;
                            Response::ToGet { id, value }
                        }
                        Request::Set { id, key, value } => {
                            let was_modified = store.set(&key, &value).await;
                            Response::ToSet { id, was_modified }
                        }
                        Request::Invalid { req_hash, msg } => Response::Error { req_hash, msg },
                    };
                    let _ = connection.write(response).await.map_err(|e| {
                        eprintln!("ERROR writing response: {}", e.to_string());
                    });
                }
                Err(e) => {
                    // TODO: bubble up and re-establish connection? report error to client?
                    eprintln!("ERROR reading request: {}", e.to_string());
                }
            }
        }
    }
}

#[cfg(test)]
mod server_tests {
    use bytes::Bytes;
    use std::net::SocketAddr;
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter};
    use tokio::sync::mpsc;

    use super::*;

    lazy_static! {
        static ref ADDR: SocketAddr = SocketAddr::from(([127, 0, 0, 1], 3000));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn handles_get_request() {
        let addr = SocketAddr::from(([127, 0, 0, 1], 3000));
        let server = Server::run(Config { addr }).await;
        let (r, w) = TcpStream::connect(addr).await.unwrap().into_split();
        let mut client_reader = BufReader::new(r);
        let client_writer = BufWriter::new(w);

        let (sx, mut rx) = mpsc::channel::<Bytes>(1);
        let get_responses = tokio::spawn(async move {
            loop {
                let mut buf: Vec<u8> = Vec::new();
                let _ = client_reader.read_until(b"\n"[0], &mut buf).await.unwrap();
                sx.send(buf.into()).await.unwrap();
            }
        });

        let request: Bytes = [r#"{"id":0,"type":"Get","key":"foo"}"#, "\n"]
            .concat()
            .into();
        let mut writer = BufWriter::new(client_writer);
        writer.write_all(&request).await.unwrap();
        writer.flush().await.unwrap();

        let expected_response: Bytes = [r#"{"id":0,"value":null}"#, "\n"].concat().into();
        let actual_response = rx.recv().await.unwrap();
        assert_eq!(expected_response, actual_response);

        get_responses.abort();
        server.abort();
    }

    // #[tokio::test]
    // #[ignore = "TODO"]
    // async fn handles_set_request() {}
    //
    // #[tokio::test]
    // #[ignore = "TODO"]
    // async fn performs_set_and_get() {}
    //
    // #[tokio::test]
    // #[ignore = "TODO"]
    // async fn handles_invalid_request() {}
    //
    // #[tokio::test]
    // #[ignore = "TODO"]
    // async fn closes_connection() {}
}
