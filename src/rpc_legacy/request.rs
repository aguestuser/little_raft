use serde::{Deserialize, Serialize};
use serde_json;

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
    Put { key: String, value: String },
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

impl TryFrom<Vec<u8>> for RpcRequest {
    type Error = serde_json::Error;
    fn try_from(bs: Vec<u8>) -> StdResult<RpcRequest, Self::Error> {
        serde_json::from_slice(&bs)
    }
}

impl Into<Vec<u8>> for RpcRequest {
    fn into(self) -> Vec<u8> {
        serde_json::to_vec(&self).unwrap()
    }
}

#[cfg(test)]
mod request_tests {
    use super::*;

    #[test]
    fn deserializing_put_request() {
        let input: Vec<u8> = r#" {"id":42,"body":{"type":"Put","key":"foo","value":"bar"}}"#.into();
        assert_eq!(
            RpcRequestEnvelope::try_from(input).unwrap(),
            RpcRequestEnvelope {
                id: 42,
                body: RpcRequest::Put {
                    key: "foo".to_string(),
                    value: "bar".to_string(),
                }
            }
        )
    }

    #[test]
    fn serializing_put_request() {
        let expected: Vec<u8> =
            r#"{"id":42,"body":{"type":"Put","key":"foo","value":"bar"}}"#.into();
        let actual: Vec<u8> = RpcRequestEnvelope {
            id: 42,
            body: RpcRequest::Put {
                key: "foo".to_string(),
                value: "bar".to_string(),
            },
        }
        .into();

        assert_eq!(expected, actual);
    }

    #[test]
    fn deserializing_invalid_request() {
        let input: Vec<u8> = "foo".into();

        assert_eq!(
            RpcRequestEnvelope::try_from(input.clone())
                .err()
                .unwrap()
                .to_string(),
            "expected ident at line 1 column 2".to_string(),
        )
    }
}
