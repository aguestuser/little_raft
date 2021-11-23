#![allow(dead_code)]
use crate::api::client::ApiClientConfig;
use crate::api::request::ApiRequest;
use crate::api::response::{ApiResponse, ApiResponseEnvelope};
use crate::rpc_legacy::client::RpcClientConfig;
use crate::rpc_legacy::request::{LegacyRpcRequest, LegacyRpcRequestEnvelope};
use crate::rpc_legacy::response::{LegacyRpcResponse, LegacyRpcResponseEnvelope};
use crate::tcp::ServerConfig;
use rand::seq::SliceRandom;
use rand::Rng;
use std::net::SocketAddr;

pub struct Gen {}

impl Gen {
    pub fn u64() -> u64 {
        let mut rng = rand::thread_rng();
        rng.gen::<u64>()
    }

    pub fn usize() -> usize {
        let mut rng = rand::thread_rng();
        rng.gen::<usize>()
    }

    pub fn str() -> String {
        let strs = vec![
            "Twas brillig and the slythy toves did gyre and gimble in the wabe.".to_string(),
            "A screaming comes across the sky.".to_string(),
            "It has happened before, but there is nothing to compare it to now.".to_string(),
            "Stately, plump Buck Mulligan came from the stairhead".to_string(),
            "bearing a bowl of lather on which a mirror and a razor lay crossed.".to_string(),
        ];
        strs.choose(&mut rand::thread_rng()).unwrap().clone()
    }

    pub fn bool() -> bool {
        vec![true, false]
            .choose(&mut rand::thread_rng())
            .unwrap()
            .clone()
    }

    pub fn socket_addr() -> SocketAddr {
        let port = port_scanner::request_open_port().unwrap();
        SocketAddr::from(([127, 0, 0, 1], port))
    }

    pub fn request_envelope() -> LegacyRpcRequestEnvelope {
        LegacyRpcRequestEnvelope {
            id: Gen::u64(),
            body: Gen::request(),
        }
    }

    pub fn request() -> LegacyRpcRequest {
        let requests = vec![LegacyRpcRequest::Put {
            key: Gen::str(),
            value: Gen::str(),
        }];
        requests.choose(&mut rand::thread_rng()).unwrap().clone()
    }

    pub fn api_response_envelope() -> ApiResponseEnvelope {
        ApiResponseEnvelope {
            id: Gen::u64(),
            body: Gen::api_response(),
        }
    }

    pub fn api_response() -> ApiResponse {
        let responses = vec![
            ApiResponse::ToGet {
                value: Some(Gen::str()),
            },
            ApiResponse::ToPut {
                was_modified: Gen::bool(),
            },
            ApiResponse::ServerError { msg: Gen::str() },
        ];
        responses.choose(&mut rand::thread_rng()).unwrap().clone()
    }

    pub fn api_response_to(req: ApiRequest) -> ApiResponse {
        match req {
            ApiRequest::Get { .. } => ApiResponse::ToGet {
                value: Some(Gen::str()),
            },
            ApiRequest::Put { .. } => ApiResponse::ToPut {
                was_modified: Gen::bool(),
            },
        }
    }

    pub fn rpc_response_envelope() -> LegacyRpcResponseEnvelope {
        LegacyRpcResponseEnvelope {
            id: Gen::u64(),
            body: Gen::rpc_response(),
        }
    }

    pub fn rpc_response() -> LegacyRpcResponse {
        let responses = vec![
            LegacyRpcResponse::ToPut {
                was_modified: Gen::bool(),
            },
            LegacyRpcResponse::ServerError { msg: Gen::str() },
        ];
        responses.choose(&mut rand::thread_rng()).unwrap().clone()
    }

    pub fn rpc_response_to(request: LegacyRpcRequest) -> LegacyRpcResponse {
        match request {
            LegacyRpcRequest::Put { .. } => LegacyRpcResponse::ToPut {
                was_modified: Gen::bool(),
            },
        }
    }

    pub fn server_config() -> ServerConfig {
        ServerConfig {
            address: Gen::socket_addr(),
        }
    }

    pub fn api_client_config() -> ApiClientConfig {
        ApiClientConfig {
            server_address: Gen::socket_addr(),
        }
    }
    pub fn rpc_client_config() -> RpcClientConfig {
        RpcClientConfig {
            peer_addresses: vec![Gen::socket_addr(), Gen::socket_addr(), Gen::socket_addr()],
        }
    }
}
