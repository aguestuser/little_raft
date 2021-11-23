use serde::{Deserialize, Serialize};
use serde_json;
use std::convert::TryFrom;
use std::result::Result as StdResult;

#[derive(Clone, Eq, PartialEq, Debug, Deserialize, Serialize, Hash)]
#[serde(deny_unknown_fields)]
pub struct RpcResponseEnvelope {
    pub id: u64,
    pub body: RpcResponse,
}

#[derive(Clone, Eq, PartialEq, Debug, Deserialize, Serialize, Hash)]
#[serde(tag = "type", deny_unknown_fields)]
pub enum RpcResponse {
    ToPut { was_modified: bool },
    ServerError { msg: String },
}

impl RpcResponse {
    pub fn display_type(&self) -> String {
        match self {
            RpcResponse::ToPut { .. } => "ToPut".to_string(),
            RpcResponse::ServerError { .. } => "Error".to_string(),
        }
    }
}

impl Into<Vec<u8>> for RpcResponseEnvelope {
    fn into(self) -> Vec<u8> {
        serde_json::to_vec(&self).unwrap()
    }
}

impl TryFrom<Vec<u8>> for RpcResponseEnvelope {
    type Error = serde_json::Error;
    fn try_from(bs: Vec<u8>) -> StdResult<RpcResponseEnvelope, Self::Error> {
        serde_json::from_slice(&bs)
    }
}

impl Into<Vec<u8>> for RpcResponse {
    fn into(self) -> Vec<u8> {
        serde_json::to_vec(&self).unwrap()
    }
}

impl TryFrom<Vec<u8>> for RpcResponse {
    type Error = serde_json::Error;
    fn try_from(bs: Vec<u8>) -> StdResult<RpcResponse, Self::Error> {
        serde_json::from_slice(&bs)
    }
}

impl RpcResponseEnvelope {
    pub fn error_of(id: u64, msg: String) -> RpcResponseEnvelope {
        RpcResponseEnvelope {
            id,
            body: RpcResponse::ServerError { msg },
        }
    }
    pub fn of_put(id: u64, was_modified: bool) -> RpcResponseEnvelope {
        RpcResponseEnvelope {
            id,
            body: { RpcResponse::ToPut { was_modified } },
        }
    }
}

#[cfg(test)]
mod response_tests {
    use super::*;

    #[test]
    fn deserializing_put_response() {
        let input: Vec<u8> = r#"{"id":42,"body":{"type":"ToPut","was_modified":true}}"#.into();

        assert_eq!(
            RpcResponseEnvelope::try_from(input).unwrap(),
            RpcResponseEnvelope {
                id: 42,
                body: RpcResponse::ToPut { was_modified: true },
            }
        );
    }

    #[test]
    fn serializing_put_response() {
        let expected: Vec<u8> = r#"{"id":42,"body":{"type":"ToPut","was_modified":true}}"#.into();
        let actual: Vec<u8> = RpcResponseEnvelope {
            id: 42,
            body: RpcResponse::ToPut { was_modified: true },
        }
        .into();
        assert_eq!(expected, actual);
    }

    #[test]
    fn serializing_error_response() {
        let expected: Vec<u8> = r#"{"id":42,"body":{"type":"ServerError","msg":"whoops!"}}"#.into();
        let actual: Vec<u8> = RpcResponseEnvelope {
            id: 42,
            body: RpcResponse::ServerError {
                msg: "whoops!".to_string(),
            },
        }
        .into();

        assert_eq!(expected, actual);
    }
}
