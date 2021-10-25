use std::net::SocketAddr;
use stors::protocol::server::Server;

#[tokio::main]
async fn main() {
    let addr = SocketAddr::from(([127, 0, 0, 1], 3000));
    let _ = Server::new(addr).run().await;
    futures::future::pending().await
}

// NOTE: if we want to change this to an http server___ (or steal ideas about semantics), see:
// https://docs.rs/hyper/0.14.13/hyper/server/index.html
