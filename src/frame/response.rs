use bytes::Bytes;
use serde::{Deserialize, Serialize};
use serde_json;

#[derive(Clone, PartialEq, Debug, Deserialize, Serialize)]
#[serde(untagged)]
pub enum Response {
    ToGet { id: u64, value: Option<String> },
    ToSet { id: u64, was_modified: bool },
    Error { req_hash: u64, msg: String },
}

impl Into<Bytes> for Response {
    fn into(self) -> Bytes {
        let mut msg = serde_json::to_vec(&self).unwrap();
        msg.push('\n' as u8);
        msg.into()
    }
}

impl From<Vec<u8>> for Response {
    fn from(bs: Vec<u8>) -> Self {
        serde_json::from_slice(&*bs).unwrap()
    }
}

#[cfg(test)]
mod response_tests {
    use super::*;

    #[test]
    fn serializing_get_response() {
        let expected: Bytes = [r#"{"id":42,"value":"bar"}"#, "\n"].concat().into();
        let actual: Bytes = Response::ToGet {
            id: 42,
            value: Some("bar".to_string()),
        }
        .into();

        assert_eq!(actual, expected);
    }

    #[test]
    fn serializing_set_response() {
        let expected: Bytes = [r#"{"id":42,"was_modified":true}"#, "\n"].concat().into();
        let actual: Bytes = Response::ToSet {
            id: 42,
            was_modified: true,
        }
        .into();
        assert_eq!(expected, actual);
    }

    #[test]
    fn serializing_error_response() {
        let expected: Bytes = [r#"{"req_hash":42,"msg":"whoops!"}"#, "\n"].concat().into();
        let actual: Bytes = Response::Error {
            req_hash: 42,
            msg: "whoops!".to_string(),
        }
        .into();

        assert_eq!(expected, actual);
    }
}
