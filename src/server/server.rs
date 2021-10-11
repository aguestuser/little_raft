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
    use std::net::SocketAddr;

    use bytes::Bytes;
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter};
    use tokio::net::tcp::OwnedWriteHalf;
    use tokio::sync::mpsc;
    use tokio::sync::mpsc::Receiver;

    use super::*;

    lazy_static! {
        static ref BUF_SIZE: usize = 10;
    }

    async fn setup() -> (BufWriter<OwnedWriteHalf>, Receiver<Bytes>) {
        let port = port_scanner::request_open_port().unwrap();
        let addr = SocketAddr::from(([127, 0, 0, 1], port));
        // TODO: return this when testing ::stop()
        let _ = Server::run(Config { addr }).await;

        let (mut client_reader, client_writer) =
            match TcpStream::connect(addr).await.unwrap().into_split() {
                (r, w) => (BufReader::new(r), BufWriter::new(w)),
            };

        let (sx, rx) = mpsc::channel::<Bytes>(*BUF_SIZE);
        tokio::spawn(async move {
            loop {
                let mut buf: Vec<u8> = Vec::new();
                let _ = client_reader.read_until(b"\n"[0], &mut buf).await.unwrap();
                sx.send(buf.into()).await.unwrap();
            }
        });

        (client_writer, rx)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn performs_get() {
        let (mut writer, mut receiver) = setup().await;

        let request: Bytes = [r#"{"id":0,"type":"Get","key":"foo"}"#, "\n"]
            .concat()
            .into();
        writer.write_all(&request).await.unwrap();
        writer.flush().await.unwrap();

        let expected_response: Bytes = [r#"{"id":0,"value":null}"#, "\n"].concat().into();
        let actual_response = receiver.recv().await.unwrap();
        assert_eq!(expected_response, actual_response);
    }

    #[tokio::test]
    async fn performs_set() {
        let (mut writer, mut receiver) = setup().await;

        let request: Bytes = [r#"{"id":0,"type":"Set","key":"foo","value":"bar"}"#, "\n"]
            .concat()
            .into();
        writer.write_all(&request).await.unwrap();
        writer.flush().await.unwrap();

        let expected_response: Bytes = [r#"{"id":0,"was_modified":true}"#, "\n"].concat().into();
        let actual_response = receiver.recv().await.unwrap();
        assert_eq!(expected_response, actual_response);
    }

    #[tokio::test]
    async fn performs_set_idempotently() {
        let (mut writer, mut receiver) = setup().await;

        let req_0: Bytes = [r#"{"id":0,"type":"Set","key":"foo","value":"bar"}"#, "\n"]
            .concat()
            .into();
        let req_1: Bytes = [r#"{"id":1,"type":"Set","key":"foo","value":"bar"}"#, "\n"]
            .concat()
            .into();

        writer.write_all(&req_0).await.unwrap();
        writer.flush().await.unwrap();
        writer.write_all(&req_1).await.unwrap();
        writer.flush().await.unwrap();

        let actual_resp_1 = receiver.recv().await.unwrap();
        let expected_resp_1: Bytes = [r#"{"id":0,"was_modified":true}"#, "\n"].concat().into();
        assert_eq!(expected_resp_1, actual_resp_1);

        let actual_resp_1 = receiver.recv().await.unwrap();
        let expected_resp_1: Bytes = [r#"{"id":1,"was_modified":false}"#, "\n"].concat().into();
        assert_eq!(expected_resp_1, actual_resp_1);
    }

    #[tokio::test]
    async fn performs_set_and_get() {
        let (mut writer, mut receiver) = setup().await;

        let get_req_0: Bytes = [r#"{"id":0,"type":"Get","key":"foo"}"#, "\n"]
            .concat()
            .into();
        let set_req_1: Bytes = [r#"{"id":1,"type":"Set","key":"foo","value":"bar"}"#, "\n"]
            .concat()
            .into();
        let get_req_2: Bytes = [r#"{"id":2,"type":"Get","key":"foo"}"#, "\n"]
            .concat()
            .into();

        writer.write_all(&get_req_0).await.unwrap();
        writer.flush().await.unwrap();
        writer.write_all(&set_req_1).await.unwrap();
        writer.flush().await.unwrap();
        writer.write_all(&get_req_2).await.unwrap();
        writer.flush().await.unwrap();

        let expected_resp_0: Bytes = [r#"{"id":0,"value":null}"#, "\n"].concat().into();
        let actual_resp_0 = receiver.recv().await.unwrap();
        assert_eq!(expected_resp_0, actual_resp_0);

        let expected_resp_1: Bytes = [r#"{"id":1,"was_modified":true}"#, "\n"].concat().into();
        let actual_resp_1 = receiver.recv().await.unwrap();
        assert_eq!(expected_resp_1, actual_resp_1);

        let expected_resp_2: Bytes = [r#"{"id":2,"value":"bar"}"#, "\n"].concat().into();
        let actual_resp_2 = receiver.recv().await.unwrap();
        assert_eq!(expected_resp_2, actual_resp_2);
    }

    #[tokio::test]
    async fn handles_invalid_request() {
        let (mut writer, mut receiver) = setup().await;

        let req: Bytes = [r#"{"id":0,"type":"Set","key":"foo"}"#, "\n"]
            .concat()
            .into();

        writer.write_all(&req).await.unwrap();
        writer.flush().await.unwrap();

        let actual_resp = receiver.recv().await.unwrap();
        let expected_resp: Bytes = [
            r#"{"req_hash":10867052927846470595,"msg":"missing field `value`"}"#,
            "\n",
        ]
        .concat()
        .into();
        assert_eq!(expected_resp, actual_resp);
    }

    #[tokio::test]
    #[ignore = "TODO"]
    async fn shuts_down_gracefully() {}
}
