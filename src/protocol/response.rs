use crate::protocol::Hasher;
use serde::{Deserialize, Serialize};
use serde_json;

#[derive(Clone, Eq, PartialEq, Debug, Deserialize, Serialize, Hash)]
#[serde(deny_unknown_fields)]
pub struct Response {
    pub id: u64,
    pub outcome: Outcome,
}

#[derive(Clone, Eq, PartialEq, Debug, Deserialize, Serialize, Hash)]
#[serde(tag = "type", deny_unknown_fields)]
pub enum Outcome {
    OfGet { value: Option<String> },
    OfPut { was_modified: bool },
    Error { msg: String },
    Invalid { msg: String },
}

impl Response {
    pub fn error_of(id: u64, msg: String) -> Response {
        Response {
            id,
            outcome: Outcome::Error { msg },
        }
    }
    pub fn of_get(id: u64, value: Option<String>) -> Response {
        Response {
            id,
            outcome: { Outcome::OfGet { value } },
        }
    }
    pub fn of_set(id: u64, was_modified: bool) -> Response {
        Response {
            id,
            outcome: { Outcome::OfPut { was_modified } },
        }
    }
    pub(crate) fn id(&self) -> u64 {
        self.id
    }
}

impl Into<Vec<u8>> for Response {
    fn into(self) -> Vec<u8> {
        serde_json::to_vec(&self).unwrap()
    }
}

impl From<Vec<u8>> for Response {
    fn from(bs: Vec<u8>) -> Self {
        serde_json::from_slice(&*bs).unwrap_or_else(|e| Response {
            id: Hasher::hash(&bs),
            outcome: Outcome::Invalid { msg: e.to_string() },
        })
    }
}

#[cfg(test)]
mod response_tests {
    use super::*;

    #[test]
    fn deserializing_get_response() {
        let input: Vec<u8> = r#"{"id":42,"outcome":{"type":"OfGet","value":"bar"}}"#.into();

        assert_eq!(
            Response::from(input),
            Response {
                id: 42,
                outcome: Outcome::OfGet {
                    value: Some("bar".to_string()),
                }
            }
        );
    }

    #[test]
    fn serializing_get_response() {
        let expected: Vec<u8> = r#"{"id":42,"outcome":{"type":"OfGet","value":"bar"}}"#.into();
        let actual: Vec<u8> = Response {
            id: 42,
            outcome: Outcome::OfGet {
                value: Some("bar".to_string()),
            },
        }
        .into();

        assert_eq!(actual, expected);
    }

    #[test]
    fn deserializing_put_response() {
        let input: Vec<u8> = r#"{"id":42,"outcome":{"type":"OfPut","was_modified":true}}"#.into();

        assert_eq!(
            Response::from(input),
            Response {
                id: 42,
                outcome: Outcome::OfPut { was_modified: true },
            }
        );
    }

    #[test]
    fn serializing_put_response() {
        let expected: Vec<u8> =
            r#"{"id":42,"outcome":{"type":"OfPut","was_modified":true}}"#.into();
        let actual: Vec<u8> = Response {
            id: 42,
            outcome: Outcome::OfPut { was_modified: true },
        }
        .into();
        assert_eq!(expected, actual);
    }

    #[test]
    fn serializing_error_response() {
        let expected: Vec<u8> = r#"{"id":42,"outcome":{"type":"Error","msg":"whoops!"}}"#.into();
        let actual: Vec<u8> = Response {
            id: 42,
            outcome: Outcome::Error {
                msg: "whoops!".to_string(),
            },
        }
        .into();

        assert_eq!(expected, actual);
    }
}
