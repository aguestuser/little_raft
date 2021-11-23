pub use crate::rpc_legacy::request::{RpcRequest, RpcRequestEnvelope};
pub use crate::rpc_legacy::response::{RpcResponse, RpcResponseEnvelope};
use crate::tcp::connection::Connection;

pub mod client;
pub mod request;
pub mod response;
pub mod server;

pub type RpcClientConnection = Connection<RpcResponseEnvelope, RpcRequestEnvelope>;
pub type RpcServerConnection = Connection<RpcRequestEnvelope, RpcResponseEnvelope>;
