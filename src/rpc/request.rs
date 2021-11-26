use serde::{Deserialize, Serialize};
use std::convert::TryFrom;
use std::result::Result as StdResult;

use crate::state::log::LogEntry;
use crate::tcp_serializable;

#[derive(Clone, Eq, PartialEq, Debug, Deserialize, Serialize, Hash)]
#[serde(deny_unknown_fields)]
pub struct RpcRequestEnvelope {
    pub id: u64,
    pub request: RpcRequest,
}
tcp_serializable!(RpcRequestEnvelope);

#[derive(Clone, Eq, PartialEq, Debug, Deserialize, Serialize, Hash)]
#[serde(tag = "type", deny_unknown_fields)]
pub enum RpcRequest {
    AppendEntries(AppendEntriesRequest),
}
tcp_serializable!(RpcRequest);

#[derive(Clone, Eq, PartialEq, Debug, Deserialize, Serialize, Hash)]
pub struct AppendEntriesRequest {
    pub entries: Vec<LogEntry>, // log entries to store (empty for heartbeat; may send more than one for efficiency)
    pub leader_address: String, // so follower can redirect clients
    pub leader_commit: usize,   // index of last log entry leader has committed
    pub leader_term: usize,     // leader’s term
    pub prev_log_index: usize,  // index of log entry immediately preceding new ones
    pub prev_log_term: usize,   // term of prevLogIndex entry
}
