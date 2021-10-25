use serde::{Deserialize, Serialize};
use serde_json;

#[derive(Clone, PartialEq, Debug, Deserialize, Serialize)]
#[serde(tag = "type", deny_unknown_fields)]
pub enum Response {
    ToGet { id: u64, value: Option<String> },
    ToSet { id: u64, was_modified: bool },
    Error { req_hash: u64, msg: String },
    Invalid { msg: String },
}

impl Into<Vec<u8>> for Response {
    fn into(self) -> Vec<u8> {
        serde_json::to_vec(&self).unwrap()
    }
}

impl From<Vec<u8>> for Response {
    fn from(bs: Vec<u8>) -> Self {
        serde_json::from_slice(&*bs).unwrap_or_else(|e| Response::Invalid { msg: e.to_string() })
    }
}

#[cfg(test)]
mod response_tests {
    use super::*;

    #[test]
    fn deserializing_get_response() {
        let input: Vec<u8> = r#"{"type":"ToGet","id":42,"value":"bar"}"#.into();

        assert_eq!(
            Response::from(input),
            Response::ToGet {
                id: 42,
                value: Some("bar".to_string()),
            }
        );
    }

    #[test]
    fn serializing_get_response() {
        let expected: Vec<u8> = r#"{"type":"ToGet","id":42,"value":"bar"}"#.into();
        let actual: Vec<u8> = Response::ToGet {
            id: 42,
            value: Some("bar".to_string()),
        }
        .into();

        assert_eq!(actual, expected);
    }

    #[test]
    fn serializing_set_response() {
        let expected: Vec<u8> = r#"{"type":"ToSet","id":42,"was_modified":true}"#.into();
        let actual: Vec<u8> = Response::ToSet {
            id: 42,
            was_modified: true,
        }
        .into();
        assert_eq!(expected, actual);
    }

    #[test]
    fn serializing_error_response() {
        let expected: Vec<u8> = r#"{"type":"Error","req_hash":42,"msg":"whoops!"}"#.into();
        let actual: Vec<u8> = Response::Error {
            req_hash: 42,
            msg: "whoops!".to_string(),
        }
        .into();

        assert_eq!(expected, actual);
    }
}
