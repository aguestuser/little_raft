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
    ToGet { value: Option<String> },
    ToPut { was_modified: bool },
    Error { msg: String },
}

impl ResponseEnvelope {
    pub fn error_of(id: u64, msg: String) -> ResponseEnvelope {
        ResponseEnvelope {
            id,
            body: Response::Error { msg },
        }
    }
    pub fn of_get(id: u64, value: Option<String>) -> ResponseEnvelope {
        ResponseEnvelope {
            id,
            body: { Response::ToGet { value } },
        }
    }
    pub fn of_set(id: u64, was_modified: bool) -> ResponseEnvelope {
        ResponseEnvelope {
            id,
            body: { Response::ToPut { was_modified } },
        }
    }
    pub(crate) fn id(&self) -> u64 {
        self.id
    }
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

#[cfg(test)]
mod response_tests {
    use super::*;

    #[test]
    fn deserializing_get_response() {
        let input: Vec<u8> = r#"{"id":42,"body":{"type":"ToGet","value":"bar"}}"#.into();

        assert_eq!(
            ResponseEnvelope::try_from(input).unwrap(),
            ResponseEnvelope {
                id: 42,
                body: Response::ToGet {
                    value: Some("bar".to_string()),
                }
            }
        );
    }

    #[test]
    fn serializing_get_response() {
        let expected: Vec<u8> = r#"{"id":42,"body":{"type":"ToGet","value":"bar"}}"#.into();
        let actual: Vec<u8> = ResponseEnvelope {
            id: 42,
            body: Response::ToGet {
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
            ResponseEnvelope::try_from(input).unwrap(),
            ResponseEnvelope {
                id: 42,
                body: Response::ToPut { was_modified: true },
            }
        );
    }

    #[test]
    fn serializing_put_response() {
        let expected: Vec<u8> = r#"{"id":42,"body":{"type":"ToPut","was_modified":true}}"#.into();
        let actual: Vec<u8> = ResponseEnvelope {
            id: 42,
            body: Response::ToPut { was_modified: true },
        }
        .into();
        assert_eq!(expected, actual);
    }

    #[test]
    fn serializing_error_response() {
        let expected: Vec<u8> = r#"{"id":42,"body":{"type":"Error","msg":"whoops!"}}"#.into();
        let actual: Vec<u8> = ResponseEnvelope {
            id: 42,
            body: Response::Error {
                msg: "whoops!".to_string(),
            },
        }
        .into();

        assert_eq!(expected, actual);
    }
}
