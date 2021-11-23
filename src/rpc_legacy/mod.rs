pub use crate::rpc_legacy::request::{LegacyRpcRequest, LegacyRpcRequestEnvelope};
pub use crate::rpc_legacy::response::{LegacyRpcResponse, LegacyRpcResponseEnvelope};
use crate::tcp::connection::Connection;

pub mod client;
pub mod request;
pub mod response;
pub mod server;

pub type LegacyRpcClientConnection =
    Connection<LegacyRpcResponseEnvelope, LegacyRpcRequestEnvelope>;
pub type LegacyRpcServerConnection =
    Connection<LegacyRpcRequestEnvelope, LegacyRpcResponseEnvelope>;
