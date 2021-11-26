use std::convert::TryFrom;
use std::result::Result as StdResult;

use serde::{Deserialize, Serialize};

use crate::tcp_serializable;

#[derive(Clone, Eq, PartialEq, Debug, Deserialize, Serialize, Hash)]
#[serde(deny_unknown_fields)]
pub struct RpcResponseEnvelope {
    pub id: u64,
    pub response: RpcResponse,
}
tcp_serializable!(RpcResponseEnvelope);

#[derive(Clone, Eq, PartialEq, Debug, Deserialize, Serialize, Hash)]
#[serde(tag = "type", deny_unknown_fields)]
pub enum RpcResponse {
    ToAppendEntries(AppendEntriesResponse),
}
tcp_serializable!(RpcResponse);

#[derive(Clone, Eq, PartialEq, Debug, Deserialize, Serialize, Hash)]
pub struct AppendEntriesResponse {
    pub peer_term: usize, // currentTerm, for leader to update itself
    pub success: bool,    // true if follower contained entry matching prevLogIndex and prevLogTerm
}

impl RpcResponseEnvelope {
    pub fn of_append_entry(id: u64, response: AppendEntriesResponse) -> RpcResponseEnvelope {
        Self {
            id,
            response: RpcResponse::ToAppendEntries(response),
        }
    }
}
