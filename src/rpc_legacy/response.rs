use serde::{Deserialize, Serialize};
use serde_json;
use std::convert::TryFrom;
use std::result::Result as StdResult;

#[derive(Clone, Eq, PartialEq, Debug, Deserialize, Serialize, Hash)]
#[serde(deny_unknown_fields)]
pub struct LegacyRpcResponseEnvelope {
    pub id: u64,
    pub body: LegacyRpcResponse,
}

#[derive(Clone, Eq, PartialEq, Debug, Deserialize, Serialize, Hash)]
#[serde(tag = "type", deny_unknown_fields)]
pub enum LegacyRpcResponse {
    ToPut { was_modified: bool },
    ServerError { msg: String },
}

impl LegacyRpcResponse {
    pub fn display_type(&self) -> String {
        match self {
            LegacyRpcResponse::ToPut { .. } => "ToPut".to_string(),
            LegacyRpcResponse::ServerError { .. } => "Error".to_string(),
        }
    }
}

impl Into<Vec<u8>> for LegacyRpcResponseEnvelope {
    fn into(self) -> Vec<u8> {
        serde_json::to_vec(&self).unwrap()
    }
}

impl TryFrom<Vec<u8>> for LegacyRpcResponseEnvelope {
    type Error = serde_json::Error;
    fn try_from(bs: Vec<u8>) -> StdResult<LegacyRpcResponseEnvelope, Self::Error> {
        serde_json::from_slice(&bs)
    }
}

impl Into<Vec<u8>> for LegacyRpcResponse {
    fn into(self) -> Vec<u8> {
        serde_json::to_vec(&self).unwrap()
    }
}

impl TryFrom<Vec<u8>> for LegacyRpcResponse {
    type Error = serde_json::Error;
    fn try_from(bs: Vec<u8>) -> StdResult<LegacyRpcResponse, Self::Error> {
        serde_json::from_slice(&bs)
    }
}

impl LegacyRpcResponseEnvelope {
    pub fn error_of(id: u64, msg: String) -> LegacyRpcResponseEnvelope {
        LegacyRpcResponseEnvelope {
            id,
            body: LegacyRpcResponse::ServerError { msg },
        }
    }
    pub fn of_put(id: u64, was_modified: bool) -> LegacyRpcResponseEnvelope {
        LegacyRpcResponseEnvelope {
            id,
            body: { LegacyRpcResponse::ToPut { was_modified } },
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
            LegacyRpcResponseEnvelope::try_from(input).unwrap(),
            LegacyRpcResponseEnvelope {
                id: 42,
                body: LegacyRpcResponse::ToPut { was_modified: true },
            }
        );
    }

    #[test]
    fn serializing_put_response() {
        let expected: Vec<u8> = r#"{"id":42,"body":{"type":"ToPut","was_modified":true}}"#.into();
        let actual: Vec<u8> = LegacyRpcResponseEnvelope {
            id: 42,
            body: LegacyRpcResponse::ToPut { was_modified: true },
        }
        .into();
        assert_eq!(expected, actual);
    }

    #[test]
    fn serializing_error_response() {
        let expected: Vec<u8> = r#"{"id":42,"body":{"type":"ServerError","msg":"whoops!"}}"#.into();
        let actual: Vec<u8> = LegacyRpcResponseEnvelope {
            id: 42,
            body: LegacyRpcResponse::ServerError {
                msg: "whoops!".to_string(),
            },
        }
        .into();

        assert_eq!(expected, actual);
    }
}
