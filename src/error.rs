#[cfg(feature = "std")]
use std::error::Error;
#[cfg(not(feature = "std"))]
use std::fmt::Display;

use err_derive::Error;

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[derive(Debug, Error)]
pub enum IllegalStateError {
    #[error(display = "no record of peer at address: {:?}", _0)]
    NoPeerAtAddress(String),
}

//
// #[derive(Debug, Error)]
// pub enum FormatError {
//     #[error(display = "invalid header (expected: {:?}, got: {:?})", expected, found)]
//     InvalidHeader {
//         expected: String,
//         found: String,
//     },
//     #[error(display = "missing attribute: {:?}", _0)]
//     MissingAttribute(String),
// }
//
