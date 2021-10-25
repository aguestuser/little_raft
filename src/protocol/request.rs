use crate::protocol::Hasher;
use serde::{Deserialize, Serialize};
use serde_json;

#[derive(Clone, PartialEq, Debug, Deserialize, Serialize)]
#[serde(tag = "type", deny_unknown_fields)]
pub enum Request {
    Get { id: u64, key: String },
    Set { id: u64, key: String, value: String },
    Invalid { req_hash: u64, msg: String },
}

impl From<Vec<u8>> for Request {
    fn from(bs: Vec<u8>) -> Request {
        serde_json::from_slice(&bs).unwrap_or_else(|e| Request::Invalid {
            req_hash: Hasher::hash(&bs),
            msg: e.to_string(),
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

    #[test]
    fn deserializing_get_request() {
        let input: Vec<u8> = r#"{"type":"Get","id":42,"key":"foo"}"#.into();

        assert_eq!(
            Request::from(input),
            Request::Get {
                id: 42,
                key: "foo".to_string()
            }
        );
    }

    #[test]
    fn serializing_get_request() {
        let expected: Vec<u8> = r#"{"type":"Get","id":42,"key":"foo"}"#.into();
        let actual: Vec<u8> = Request::Get {
            id: 42,
            key: "foo".to_string(),
        }
        .into();

        assert_eq!(expected, actual);
    }

    #[test]
    fn deserializing_set_request() {
        let input: Vec<u8> = r#" { "type":"Set", "id":42, "key":"foo", "value":"bar" }"#.into();
        assert_eq!(
            Request::from(input),
            Request::Set {
                id: 42,
                key: "foo".to_string(),
                value: "bar".to_string(),
            }
        )
    }

    #[test]
    fn serializing_set_request() {
        let expected: Vec<u8> = r#"{"type":"Set","id":42,"key":"foo","value":"bar"}"#.into();
        let actual: Vec<u8> = Request::Set {
            id: 42,
            key: "foo".to_string(),
            value: "bar".to_string(),
        }
        .into();

        assert_eq!(expected, actual);
    }

    #[test]
    fn deserializing_invalid_request() {
        let input: Vec<u8> = "foo".into();

        assert_eq!(
            Request::from(input.clone()),
            Request::Invalid {
                req_hash: Hasher::hash(&input),
                msg: "expected ident at line 1 column 2".to_string(),
            },
        )
    }
}
