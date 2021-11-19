use crate::rpc::Hasher;
use serde::{Deserialize, Serialize};
use serde_json;

#[derive(Clone, Eq, PartialEq, Debug, Deserialize, Serialize, Hash)]
#[serde(deny_unknown_fields)]
pub struct Request {
    pub id: u64,
    pub command: Command,
}

#[derive(Clone, Eq, PartialEq, Debug, Deserialize, Serialize, Hash)]
#[serde(tag = "type", deny_unknown_fields)]
pub enum Command {
    Get { key: String },
    Put { key: String, value: String },
    Invalid { msg: String },
}

impl Request {
    // TODO: get rid of this
    pub fn id(&self) -> u64 {
        self.id
    }
}

impl From<Vec<u8>> for Request {
    fn from(bs: Vec<u8>) -> Request {
        serde_json::from_slice(&bs).unwrap_or_else(|e| Request {
            id: Hasher::hash(&bs),
            command: Command::Invalid { msg: e.to_string() },
        })
    }
}

impl Into<Vec<u8>> for Request {
    fn into(self) -> Vec<u8> {
        serde_json::to_vec(&self).unwrap()
    }
}

#[cfg(test)]
mod request_tests {
    use super::*;
    use crate::rpc::request::Command::Invalid;

    #[test]
    fn deserializing_get_request() {
        let input: Vec<u8> = r#"{"id":42,"command":{"type":"Get","key":"foo"}}"#.into();

        assert_eq!(
            Request::from(input),
            Request {
                id: 42,
                command: Command::Get {
                    key: "foo".to_string(),
                }
            }
        );
    }

    #[test]
    fn serializing_get_request() {
        let expected: Vec<u8> = r#"{"id":42,"command":{"type":"Get","key":"foo"}}"#.into();
        let actual: Vec<u8> = Request {
            id: 42,
            command: Command::Get {
                key: "foo".to_string(),
            },
        }
        .into();

        assert_eq!(expected, actual);
    }

    #[test]
    fn deserializing_put_request() {
        let input: Vec<u8> =
            r#" {"id":42,"command":{"type":"Put","key":"foo","value":"bar"}}"#.into();
        assert_eq!(
            Request::from(input),
            Request {
                id: 42,
                command: Command::Put {
                    key: "foo".to_string(),
                    value: "bar".to_string(),
                }
            }
        )
    }

    #[test]
    fn serializing_put_request() {
        let expected: Vec<u8> =
            r#"{"id":42,"command":{"type":"Put","key":"foo","value":"bar"}}"#.into();
        let actual: Vec<u8> = Request {
            id: 42,
            command: Command::Put {
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
            Request::from(input.clone()),
            Request {
                id: Hasher::hash(&input),
                command: Invalid {
                    msg: "expected ident at line 1 column 2".to_string(),
                }
            },
        )
    }
}
