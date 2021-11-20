use crate::api::request::RequestEnvelope;
use crate::api::response::ResponseEnvelope;
use crate::tcp::connection::Connection;

pub mod client;
pub mod request;
pub mod response;
pub mod server;

pub const REQUEST_BUFFER_SIZE: usize = 16;

pub type ClientConnection = Connection<ResponseEnvelope, RequestEnvelope>;
pub type ServerConnection = Connection<RequestEnvelope, ResponseEnvelope>;
