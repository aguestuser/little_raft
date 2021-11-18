#![allow(unused_imports)]

#[cfg(feature = "std")]
use std::error::Error;
#[cfg(not(feature = "std"))]
use std::fmt::Display;

use crate::tcp::request::Request;
use err_derive::Error;

pub type AsyncError = Box<dyn std::error::Error + Send + Sync>;
pub type Result<T> = std::result::Result<T, AsyncError>;

#[derive(Debug, Error)]
pub enum NetworkError {
    #[error(display = "no record of peer at address: {:?}", _0)]
    NoPeerAtAddress(String),
    #[error(display = "peer connection closed")]
    PeerConnectionClosed,
    #[error(display = "request timed out")]
    RequestTimeout,
    #[error(display = "broadcast failed to receive successful response from majority of peers")]
    BroadcastFailure,
    #[error(display = "parallel requests failed to join")]
    TaskJoinFailure,
}

#[derive(Debug, Error)]
pub enum PermissionError {
    #[error(display = "followers are not permitted to issue Get commands")]
    FollowersMayNotGet,
}

#[derive(Debug, Error)]
pub enum PersistenceError {
    #[error(display = "failed to insert value into store")]
    InsertionError,
    #[error(display = "failed to retrieve value from store")]
    RetrievalError,
}
