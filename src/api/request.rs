use serde::{Deserialize, Serialize};
use serde_json;

use std::convert::TryFrom;
use std::result::Result as StdResult;

#[derive(Clone, Eq, PartialEq, Debug, Deserialize, Serialize, Hash)]
#[serde(deny_unknown_fields)]
pub struct ApiRequestEnvelope {
    pub id: u64,
    pub body: ApiRequest,
}

#[derive(Clone, Eq, PartialEq, Debug, Deserialize, Serialize, Hash)]
#[serde(tag = "type", deny_unknown_fields)]
pub enum ApiRequest {
    Get { key: String },
    Put { key: String, value: String },
}

impl TryFrom<Vec<u8>> for ApiRequestEnvelope {
    type Error = serde_json::Error;
    fn try_from(bs: Vec<u8>) -> StdResult<ApiRequestEnvelope, Self::Error> {
        serde_json::from_slice(&bs)
    }
}

impl Into<Vec<u8>> for ApiRequestEnvelope {
    fn into(self) -> Vec<u8> {
        serde_json::to_vec(&self).unwrap()
    }
}

#[cfg(test)]
mod request_tests {
    use super::*;

    #[test]
    fn deserializing_get_request() {
        let input: Vec<u8> = r#"{"id":42,"body":{"type":"Get","key":"foo"}}"#.into();

        assert_eq!(
            ApiRequestEnvelope::try_from(input).unwrap(),
            ApiRequestEnvelope {
                id: 42,
                body: ApiRequest::Get {
                    key: "foo".to_string(),
                }
            }
        );
    }

    #[test]
    fn serializing_get_request() {
        let expected: Vec<u8> = r#"{"id":42,"body":{"type":"Get","key":"foo"}}"#.into();
        let actual: Vec<u8> = ApiRequestEnvelope {
            id: 42,
            body: ApiRequest::Get {
                key: "foo".to_string(),
            },
        }
        .into();

        assert_eq!(expected, actual);
    }

    #[test]
    fn deserializing_put_request() {
        let input: Vec<u8> = r#" {"id":42,"body":{"type":"Put","key":"foo","value":"bar"}}"#.into();
        assert_eq!(
            ApiRequestEnvelope::try_from(input).unwrap(),
            ApiRequestEnvelope {
                id: 42,
                body: ApiRequest::Put {
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
        let actual: Vec<u8> = ApiRequestEnvelope {
            id: 42,
            body: ApiRequest::Put {
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
            ApiRequestEnvelope::try_from(input.clone())
                .err()
                .unwrap()
                .to_string(),
            "expected ident at line 1 column 2".to_string(),
        )
    }
}
