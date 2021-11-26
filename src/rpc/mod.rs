use crate::rpc::request::RpcRequestEnvelope;
use crate::rpc::response::RpcResponseEnvelope;
use crate::tcp::Connection;

pub mod client;
pub mod request;
pub mod response;
pub mod server;

pub type RpcClientConnection = Connection<RpcResponseEnvelope, RpcRequestEnvelope>;
pub type RpcServerConnection = Connection<RpcRequestEnvelope, RpcResponseEnvelope>;
