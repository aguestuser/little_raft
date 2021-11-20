use serde::{Deserialize, Serialize};
use serde_json;

use crate::state::log::LogEntry;
use std::convert::TryFrom;
use std::result::Result as StdResult;

#[derive(Clone, Eq, PartialEq, Debug, Deserialize, Serialize, Hash)]
#[serde(deny_unknown_fields)]
pub struct RpcRequestEnvelope {
    pub id: u64,
    pub body: RpcRequest,
}

#[derive(Clone, Eq, PartialEq, Debug, Deserialize, Serialize, Hash)]
#[serde(tag = "type", deny_unknown_fields)]
pub enum RpcRequest {
    AppendEntries {
        term: usize,            // leader’s term
        leader_id: usize,       // so follower can redirect clients
        prev_log_index: usize,  // index of log entry immediately preceding new ones
        prev_log_term: usize,   // term of prevLogIndex entry
        entries: Vec<LogEntry>, // log entries to store (empty for heartbeat; may send more than one for efficiency)
        leader_commit: usize,   // leader’s commitIndex
    },
}

impl TryFrom<Vec<u8>> for RpcRequestEnvelope {
    type Error = serde_json::Error;
    fn try_from(bs: Vec<u8>) -> StdResult<RpcRequestEnvelope, Self::Error> {
        serde_json::from_slice(&bs)
    }
}

impl Into<Vec<u8>> for RpcRequestEnvelope {
    fn into(self) -> Vec<u8> {
        serde_json::to_vec(&self).unwrap()
    }
}
