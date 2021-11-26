use std::convert::TryFrom;
use std::result::Result as StdResult;

use serde::{Deserialize, Serialize};
use serde_json;

use crate::tcp_serializable;

#[derive(Clone, Eq, PartialEq, Debug, Deserialize, Serialize, Hash)]
#[serde(deny_unknown_fields)]
pub struct ApiResponseEnvelope {
    pub id: u64,
    pub response: ApiResponse,
}
tcp_serializable!(ApiResponseEnvelope);

#[derive(Clone, Eq, PartialEq, Debug, Deserialize, Serialize, Hash)]
#[serde(tag = "type", deny_unknown_fields)]
pub enum ApiResponse {
    ToGet { value: Option<String> },
    ToPut { was_modified: bool },
    Redirect { leader_address: String },
    ServerError { msg: String },
}
tcp_serializable!(ApiResponse);

impl ApiResponse {
    pub fn display_type(&self) -> String {
        match self {
            ApiResponse::ToGet { .. } => "ToGet".to_string(),
            ApiResponse::ToPut { .. } => "ToPut".to_string(),
            ApiResponse::Redirect { .. } => "Redirect".to_string(),
            ApiResponse::ServerError { .. } => "ServerError".to_string(),
        }
    }
}

impl ApiResponseEnvelope {
    pub fn error_of(id: u64, msg: String) -> ApiResponseEnvelope {
        ApiResponseEnvelope {
            id,
            response: ApiResponse::ServerError { msg },
        }
    }
    pub fn of_get(id: u64, value: Option<String>) -> ApiResponseEnvelope {
        ApiResponseEnvelope {
            id,
            response: { ApiResponse::ToGet { value } },
        }
    }
    pub fn of_put(id: u64, was_modified: bool) -> ApiResponseEnvelope {
        ApiResponseEnvelope {
            id,
            response: { ApiResponse::ToPut { was_modified } },
        }
    }
    pub fn of_redirect(id: u64, leader_address: String) -> ApiResponseEnvelope {
        ApiResponseEnvelope {
            id,
            response: { ApiResponse::Redirect { leader_address } },
        }
    }
}

#[cfg(test)]
mod response_tests {
    use super::*;

    #[test]
    fn deserializing_get_response() {
        let input: Vec<u8> = r#"{"id":42,"response":{"type":"ToGet","value":"bar"}}"#.into();

        assert_eq!(
            ApiResponseEnvelope::try_from(input).unwrap(),
            ApiResponseEnvelope {
                id: 42,
                response: ApiResponse::ToGet {
                    value: Some("bar".to_string()),
                }
            }
        );
    }

    #[test]
    fn serializing_get_response() {
        let expected: Vec<u8> = r#"{"id":42,"response":{"type":"ToGet","value":"bar"}}"#.into();
        let actual: Vec<u8> = ApiResponseEnvelope {
            id: 42,
            response: ApiResponse::ToGet {
                value: Some("bar".to_string()),
            },
        }
        .into();
        assert_eq!(actual, expected);
    }

    #[test]
    fn deserializing_put_response() {
        let input: Vec<u8> = r#"{"id":42,"response":{"type":"ToPut","was_modified":true}}"#.into();

        assert_eq!(
            ApiResponseEnvelope::try_from(input).unwrap(),
            ApiResponseEnvelope {
                id: 42,
                response: ApiResponse::ToPut { was_modified: true },
            }
        );
    }

    #[test]
    fn serializing_put_response() {
        let expected: Vec<u8> =
            r#"{"id":42,"response":{"type":"ToPut","was_modified":true}}"#.into();
        let actual: Vec<u8> = ApiResponseEnvelope {
            id: 42,
            response: ApiResponse::ToPut { was_modified: true },
        }
        .into();
        assert_eq!(expected, actual);
    }

    #[test]
    fn serializing_error_response() {
        let expected: Vec<u8> =
            r#"{"id":42,"response":{"type":"ServerError","msg":"whoops!"}}"#.into();
        let actual: Vec<u8> = ApiResponseEnvelope {
            id: 42,
            response: ApiResponse::ServerError {
                msg: "whoops!".to_string(),
            },
        }
        .into();

        assert_eq!(expected, actual);
    }
}
