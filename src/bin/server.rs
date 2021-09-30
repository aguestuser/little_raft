use std::net::SocketAddr;
use stors::server::config::Config;
use stors::server::server::Server;

#[tokio::main]
async fn main() {
    // TODO: parse configs from args!
    let config = Config {
        addr: SocketAddr::from(([127, 0, 0, 1], 3000)),
    };
    // TODO: return something other than join handle from
    let _ = Server::run(config).await;
    futures::future::pending().await
}

// NOTE: if we want to change this to an http server (or steal ideas about semantics), see:
// https://docs.rs/hyper/0.14.13/hyper/server/index.html
