use crate::api::request::ApiRequestEnvelope;
use crate::api::response::ApiResponseEnvelope;
use crate::tcp::connection::Connection;

pub mod client;
pub mod request;
pub mod response;
pub mod server;

pub const REQUEST_BUFFER_SIZE: usize = 16;

pub type ClientConnection = Connection<ApiResponseEnvelope, ApiRequestEnvelope>;
pub type ServerConnection = Connection<ApiRequestEnvelope, ApiResponseEnvelope>;
