use serde::{Deserialize, Serialize};
use serde_json;
use std::convert::TryFrom;
use std::result::Result as StdResult;

#[derive(Clone, Eq, PartialEq, Debug, Deserialize, Serialize, Hash)]
#[serde(deny_unknown_fields)]
pub struct ResponseEnvelope {
    pub id: u64,
    pub body: Response,
}

#[derive(Clone, Eq, PartialEq, Debug, Deserialize, Serialize, Hash)]
#[serde(tag = "type", deny_unknown_fields)]
pub enum Response {
    ToAppendEntry {
        term: usize,   // currentTerm, for leader to update itself
        success: bool, // true if follower contained entry matching prevLogIndex and prevLogTerm
    },
}

impl Into<Vec<u8>> for ResponseEnvelope {
    fn into(self) -> Vec<u8> {
        serde_json::to_vec(&self).unwrap()
    }
}

impl TryFrom<Vec<u8>> for ResponseEnvelope {
    type Error = serde_json::Error;
    fn try_from(bs: Vec<u8>) -> StdResult<ResponseEnvelope, Self::Error> {
        serde_json::from_slice(&bs)
    }
}
