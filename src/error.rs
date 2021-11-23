#![allow(unused_imports)]

#[cfg(feature = "std")]
use std::error::Error;
#[cfg(not(feature = "std"))]
use std::fmt::Display;

use err_derive::Error;

pub type AsyncError = Box<dyn std::error::Error + Send + Sync>;
pub type Result<T> = std::result::Result<T, AsyncError>;
macro_rules! boxed_async_err {
    ($struct_name:ident) => {
        impl $struct_name {
            pub fn boxed(self) -> AsyncError {
                Box::new(self) as AsyncError
            }
        }
    };
}

#[derive(Debug, Error)]
pub enum NetworkError {
    #[error(display = "no record of peer at address: {:?}", _0)]
    NoPeerAtAddress(String),
    #[error(display = "peer connection closed")]
    ConnectionClosed,
    #[error(display = "request timed out")]
    RequestTimeout,
    #[error(display = "broadcast failed to receive successful response from majority of peers")]
    BroadcastFailure,
    #[error(display = "parallel requests failed to join")]
    TaskJoinFailure,
    #[error(display = "failed to deserialize message from wire: {:?}", _0)]
    MessageDeserializationError(String),
}
boxed_async_err!(NetworkError);

#[derive(Debug, Error)]
pub enum ProtocolError {
    #[error(display = "unexpected response type: {:?}", _0)]
    BadResponse(String),
    #[error(display = "server failed to process request with error: {:?}", _0)]
    ServerError(String),
    #[error(
        display = "request issued to follower but must be handled by leader at: {:?}",
        _0
    )]
    LeaderRequired(String),
    #[error(display = "request issued to leader but must be handled by follower")]
    FollowerRequired,
    #[error(display = "failed to replicate command to cluster")]
    ReplicationFailed,
}
boxed_async_err!(ProtocolError);

#[derive(Debug, Error)]
pub enum PermissionError {
    #[error(display = "followers are not permitted to issue Get requests")]
    FollowersMayNotGet,
}
boxed_async_err!(PermissionError);

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
boxed_async_err!(PersistenceError);
