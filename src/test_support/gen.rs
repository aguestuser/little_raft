#![allow(dead_code)]
use crate::api::client::ClientConfig;
use crate::api::request::{ApiRequest, ApiRequestEnvelope};
use crate::api::response::{ApiResponse, ApiResponseEnvelope};
use crate::api::server::ServerConfig;
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

    pub fn request_envelope() -> ApiRequestEnvelope {
        ApiRequestEnvelope {
            id: Gen::u64(),
            body: Gen::request(),
        }
    }

    pub fn request() -> ApiRequest {
        let requests = vec![
            ApiRequest::Get { key: Gen::str() },
            ApiRequest::Put {
                key: Gen::str(),
                value: Gen::str(),
            },
        ];
        requests.choose(&mut rand::thread_rng()).unwrap().clone()
    }

    pub fn response_envelope() -> ApiResponseEnvelope {
        ApiResponseEnvelope {
            id: Gen::u64(),
            body: ApiResponse::ToGet {
                value: Some(Gen::str()),
            },
        }
    }

    pub fn response() -> ApiResponse {
        let responses = vec![
            ApiResponse::ToGet {
                value: Some(Gen::str()),
            },
            ApiResponse::ToPut {
                was_modified: Gen::bool(),
            },
            ApiResponse::Error { msg: Gen::str() },
        ];
        responses.choose(&mut rand::thread_rng()).unwrap().clone()
    }

    pub fn response_to(request: ApiRequest) -> ApiResponse {
        match request {
            ApiRequest::Get { .. } => ApiResponse::ToGet {
                value: Some(Gen::str()),
            },
            ApiRequest::Put { .. } => ApiResponse::ToPut {
                was_modified: Gen::bool(),
            },
        }
    }

    pub fn server_config() -> ServerConfig {
        ServerConfig {
            address: Gen::socket_addr(),
        }
    }

    pub fn client_config() -> ClientConfig {
        ClientConfig {
            peer_addresses: vec![Gen::socket_addr(), Gen::socket_addr(), Gen::socket_addr()],
        }
    }
}
