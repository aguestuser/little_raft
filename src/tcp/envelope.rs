// use serde::{Deserialize, Serialize};
// use std::convert::TryFrom;
// use std::fmt::Debug;
// use std::hash::Hash;
// use std::result::Result as StdResult;
//
// pub struct Envelope<Body>
// where
//     Body: Into<Vec<u8>> + TryFrom<Vec<u8>>,
// {
//     pub id: u64,
//     pub body: Body,
// }
//
// impl<Body> Into<Vec<u8>> for Envelope<Body>
// where
//     Body: Into<Vec<u8>> + TryFrom<Vec<u8>>,
// {
//     fn into(self) -> Vec<u8> {
//         [self.id.to_le_bytes().to_vec(), self.body.into()].concat()
//     }
// }
//
// impl<Body> TryFrom<Vec<u8>> for Envelope<Body>
// where
//     Body: Into<Vec<u8>> + TryFrom<Vec<u8>>,
// {
//     type Error = serde_json::Error;
//     fn try_from(bs: Vec<u8>) -> StdResult<Envelope<Body>, Self::Error> {
//         let id_bytes: [u8] = bs[..8];
//         let id: u64 = u64::from_le_bytes(*id_bytes);
//
//         let body_bytes = bs[8..];
//         let body: Body = body_bytes.try_from()?;
//
//         Ok(Envelope { id, body })
//     }
// }
