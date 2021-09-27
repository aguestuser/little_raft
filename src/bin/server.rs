use std::net::SocketAddr;
use hyper::{Body, Request, Response, Server, Method, StatusCode};
use hyper::service::{make_service_fn, service_fn};
use std::convert::Infallible;


#[tokio::main]
async fn main() {
    // TODO: read ip, port from argv
    let ip = [127, 0, 0, 1];
    let port = 3000;

    let addr = SocketAddr::from((ip, port));
    let server = Server::bind(&addr)
        .serve(make_service_fn(|_conn| async {
            Ok::<_, Infallible>(service_fn(handle_req))
        }));
    println!("> LISTENING on {:?}:{}", ip, port);

    if let Err(e) = server.await {
        eprintln!("> ERROR: {}", e);
    }
}

async fn handle_req(req: Request<Body>) -> Result<Response<Body>, Infallible> {
    log(&req);
    let mut resp = Response::new(Body::empty());

    match (req.method(), req.uri().path()) {
        (&Method::GET, "/") => {
            *resp.body_mut() = Body::from("Whoops! Try POST-ing data to /echo");
        }
        (&Method::POST, "/echo") => {
            *resp.body_mut() = req.into_body();
        }
        _ => {
            *resp.status_mut() = StatusCode::NOT_FOUND;
        }
    }

    Ok(resp)
}

fn log(req: &Request<Body>) {
    println!("> GOT REQUEST: {:?}", req);
}