#![allow(dead_code)]
use crate::api::client::ApiClientConfig;
use crate::api::request::{ApiRequest, ApiRequestEnvelope};
use crate::api::response::{ApiResponse, ApiResponseEnvelope};
use crate::rpc::client::RpcClientConfig;
use crate::rpc::request::{AppendEntriesRequest, RpcRequest, RpcRequestEnvelope};
use crate::rpc::response::{AppendEntriesResponse, RpcResponse, RpcResponseEnvelope};
use crate::state::log::{Command, LogEntry};
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
            "foo".to_string(),
            "bar".to_string(),
            "baz".to_string(),
            "bam".to_string(),
            "qux".to_string(),
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

    pub fn rpc_request_envelope() -> RpcRequestEnvelope {
        RpcRequestEnvelope {
            id: Gen::u64(),
            request: Gen::rpc_request(),
        }
    }

    pub fn rpc_request() -> RpcRequest {
        let requests = vec![RpcRequest::AppendEntries(AppendEntriesRequest {
            entries: vec![],
            leader_address: Gen::socket_addr().to_string(),
            leader_commit: 0,
            leader_term: 0,
            prev_log_index: 0,
            prev_log_term: 0,
        })];
        requests.choose(&mut rand::thread_rng()).unwrap().clone()
    }

    pub fn log_entries(num_entries: usize) -> Vec<LogEntry> {
        (0..num_entries).map(Gen::log_entry_with_term).collect()
    }

    pub fn log_entry_with_term(term: usize) -> LogEntry {
        LogEntry {
            term,
            command: Gen::put_cmd(),
        }
    }

    pub fn log_entry() -> LogEntry {
        LogEntry {
            term: Gen::usize(),
            command: Gen::put_cmd(),
        }
    }

    pub fn put_cmd() -> Command {
        Command::Put {
            key: Gen::str(),
            value: Gen::str(),
        }
    }

    pub fn api_request_envelope() -> ApiRequestEnvelope {
        ApiRequestEnvelope {
            id: Gen::u64(),
            request: Gen::api_request(),
        }
    }

    pub fn api_request() -> ApiRequest {
        let requests = [
            ApiRequest::Put {
                key: Gen::str(),
                value: Gen::str(),
            },
            ApiRequest::Get { key: Gen::str() },
        ];
        requests.choose(&mut rand::thread_rng()).unwrap().clone()
    }

    pub fn api_response_envelope() -> ApiResponseEnvelope {
        ApiResponseEnvelope {
            id: Gen::u64(),
            response: Gen::api_response(),
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

    pub fn rpc_response_envelope() -> RpcResponseEnvelope {
        RpcResponseEnvelope {
            id: Gen::u64(),
            response: Gen::rpc_response(),
        }
    }

    pub fn rpc_response() -> RpcResponse {
        let responses = vec![RpcResponse::ToAppendEntries(AppendEntriesResponse {
            peer_term: 0,
            success: true,
        })];
        responses.choose(&mut rand::thread_rng()).unwrap().clone()
    }

    pub fn rpc_response_to(request: RpcRequest) -> RpcResponse {
        match request {
            RpcRequest::AppendEntries { .. } => {
                RpcResponse::ToAppendEntries(AppendEntriesResponse {
                    peer_term: 0,
                    success: true,
                })
            }
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
