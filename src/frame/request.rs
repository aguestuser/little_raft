use crate::frame::hasher::Hasher;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use serde_json;

#[derive(Clone, PartialEq, Debug, Deserialize, Serialize)]
#[serde(tag = "type", deny_unknown_fields)]
pub enum Request {
    Get { id: u64, key: String },
    Set { id: u64, key: String, value: String },
    Invalid { req_hash: u64, msg: String },
}

impl From<Bytes> for Request {
    fn from(frame: Bytes) -> Request {
        serde_json::from_slice(&*frame).map_or_else(
            |e| Request::Invalid {
                req_hash: Hasher::hash(frame),
                msg: e.to_string(),
            },
            |req| req,
        )
    }
}

impl From<Vec<u8>> for Request {
    fn from(bytes: Vec<u8>) -> Self {
        let bs: Bytes = bytes.into();
        return bs.into();
    }
}

impl Into<Bytes> for Request {
    fn into(self) -> Bytes {
        serde_json::to_vec(&self).unwrap().into()
    }
}

#[cfg(test)]
mod request_tests {
    use super::*;

    #[test]
    fn deserializing_get_request() {
        let input: Bytes = r#"{"id":42,"type":"Get","key":"foo"}"#.into();

        assert_eq!(
            Request::from(input),
            Request::Get {
                id: 42,
                key: "foo".to_string()
            }
        );
    }

    #[test]
    fn deserializing_set_request() {
        let input: Bytes = r#"
        {
          "type":"Set",
          "id":42,
          "key":"foo",
          "value":"bar"
        }
        "#
        .into();

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
    fn deserializing_invalid_request() {
        let input: Bytes = "foo".into();

        assert_eq!(
            Request::from(input.clone()),
            Request::Invalid {
                req_hash: Hasher::hash(input),
                msg: "expected ident at line 1 column 2".to_string(),
            },
        )
    }
}
