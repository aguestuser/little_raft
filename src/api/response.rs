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
    Redirect { leader_address: String },
    ServerError { msg: String },
}

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

impl Into<Vec<u8>> for ApiResponse {
    fn into(self) -> Vec<u8> {
        serde_json::to_vec(&self).unwrap()
    }
}

impl TryFrom<Vec<u8>> for ApiResponse {
    type Error = serde_json::Error;
    fn try_from(bs: Vec<u8>) -> StdResult<ApiResponse, Self::Error> {
        serde_json::from_slice(&bs)
    }
}

impl ApiResponseEnvelope {
    pub fn error_of(id: u64, msg: String) -> ApiResponseEnvelope {
        ApiResponseEnvelope {
            id,
            body: ApiResponse::ServerError { msg },
        }
    }
    pub fn of_get(id: u64, value: Option<String>) -> ApiResponseEnvelope {
        ApiResponseEnvelope {
            id,
            body: { ApiResponse::ToGet { value } },
        }
    }
    pub fn of_put(id: u64, was_modified: bool) -> ApiResponseEnvelope {
        ApiResponseEnvelope {
            id,
            body: { ApiResponse::ToPut { was_modified } },
        }
    }
    pub fn of_redirect(id: u64, leader_address: String) -> ApiResponseEnvelope {
        ApiResponseEnvelope {
            id,
            body: { ApiResponse::Redirect { leader_address } },
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
        let actual_str = String::from_utf8(actual.clone()).unwrap();
        println!("actual_str: {:?}", actual_str);

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
        let expected: Vec<u8> = r#"{"id":42,"body":{"type":"ServerError","msg":"whoops!"}}"#.into();
        let actual: Vec<u8> = ApiResponseEnvelope {
            id: 42,
            body: ApiResponse::ServerError {
                msg: "whoops!".to_string(),
            },
        }
        .into();

        assert_eq!(expected, actual);
    }
}
