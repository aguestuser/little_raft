#![allow(unused_imports)]

#[cfg(feature = "std")]
use std::error::Error;
#[cfg(not(feature = "std"))]
use std::fmt::Display;

use err_derive::Error;

use crate::api::request::RequestEnvelope;

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
    #[error(display = "failed to deserialize message from wire: {:?}", _0)]
    MessageDeserializationError(String),
}

#[derive(Debug, Error)]
pub enum PermissionError {
    #[error(display = "followers are not permitted to issue Get requests")]
    FollowersMayNotGet,
}

#[derive(Debug, Error)]
pub enum PersistenceError {
    #[error(display = "failed to insert value into store")]
    InsertionError,
    #[error(display = "failed to retrieve value from store")]
    RetrievalError,
    #[error(display = "failed to deserialize entry: {:?}", _0)]
    LogDeserializationError(String),
    #[error(display = "tried to pop from empty log")]
    RemoveFromEmptyLogError,
}
