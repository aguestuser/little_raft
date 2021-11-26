use crate::api::request::ApiRequestEnvelope;
use crate::api::response::ApiResponseEnvelope;
use crate::tcp::Connection;

pub mod client;
pub mod request;
pub mod response;
pub mod server;

pub type ApiClientConnection = Connection<ApiResponseEnvelope, ApiRequestEnvelope>;
pub type ApiServerConnection = Connection<ApiRequestEnvelope, ApiResponseEnvelope>;
