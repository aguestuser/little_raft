use serde::{Deserialize, Serialize};
use serde_json;
use std::convert::TryFrom;
use std::result::Result as StdResult;

#[derive(Clone, Eq, PartialEq, Debug, Deserialize, Serialize, Hash)]
#[serde(deny_unknown_fields)]
pub struct ApiResponseEnvelope {
    pub id: u64,
    pub body: ApiResponse,
}

#[derive(Clone, Eq, PartialEq, Debug, Deserialize, Serialize, Hash)]
#[serde(tag = "type", deny_unknown_fields)]
pub enum ApiResponse {
    ToGet { value: Option<String> },
    ToPut { was_modified: bool },
    Error { msg: String },
}

impl Into<Vec<u8>> for ApiResponseEnvelope {
    fn into(self) -> Vec<u8> {
        serde_json::to_vec(&self).unwrap()
    }
}

impl TryFrom<Vec<u8>> for ApiResponseEnvelope {
    type Error = serde_json::Error;
    fn try_from(bs: Vec<u8>) -> StdResult<ApiResponseEnvelope, Self::Error> {
        serde_json::from_slice(&bs)
    }
}

impl ApiResponseEnvelope {
    pub fn error_of(id: u64, msg: String) -> ApiResponseEnvelope {
        ApiResponseEnvelope {
            id,
            body: ApiResponse::Error { msg },
        }
    }
    pub fn of_get(id: u64, value: Option<String>) -> ApiResponseEnvelope {
        ApiResponseEnvelope {
            id,
            body: { ApiResponse::ToGet { value } },
        }
    }
    pub fn of_set(id: u64, was_modified: bool) -> ApiResponseEnvelope {
        ApiResponseEnvelope {
            id,
            body: { ApiResponse::ToPut { was_modified } },
        }
    }
    pub(crate) fn id(&self) -> u64 {
        self.id
    }
}

#[cfg(test)]
mod response_tests {
    use super::*;

    #[test]
    fn deserializing_get_response() {
        let input: Vec<u8> = r#"{"id":42,"body":{"type":"ToGet","value":"bar"}}"#.into();

        assert_eq!(
            ApiResponseEnvelope::try_from(input).unwrap(),
            ApiResponseEnvelope {
                id: 42,
                body: ApiResponse::ToGet {
                    value: Some("bar".to_string()),
                }
            }
        );
    }

    #[test]
    fn serializing_get_response() {
        let expected: Vec<u8> = r#"{"id":42,"body":{"type":"ToGet","value":"bar"}}"#.into();
        let actual: Vec<u8> = ApiResponseEnvelope {
            id: 42,
            body: ApiResponse::ToGet {
                value: Some("bar".to_string()),
            },
        }
        .into();

        assert_eq!(actual, expected);
    }

    #[test]
    fn deserializing_put_response() {
        let input: Vec<u8> = r#"{"id":42,"body":{"type":"ToPut","was_modified":true}}"#.into();

        assert_eq!(
            ApiResponseEnvelope::try_from(input).unwrap(),
            ApiResponseEnvelope {
                id: 42,
                body: ApiResponse::ToPut { was_modified: true },
            }
        );
    }

    #[test]
    fn serializing_put_response() {
        let expected: Vec<u8> = r#"{"id":42,"body":{"type":"ToPut","was_modified":true}}"#.into();
        let actual: Vec<u8> = ApiResponseEnvelope {
            id: 42,
            body: ApiResponse::ToPut { was_modified: true },
        }
        .into();
        assert_eq!(expected, actual);
    }

    #[test]
    fn serializing_error_response() {
        let expected: Vec<u8> = r#"{"id":42,"body":{"type":"Error","msg":"whoops!"}}"#.into();
        let actual: Vec<u8> = ApiResponseEnvelope {
            id: 42,
            body: ApiResponse::Error {
                msg: "whoops!".to_string(),
            },
        }
        .into();

        assert_eq!(expected, actual);
    }
}
