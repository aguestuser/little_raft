#[cfg(test)]
#[macro_use]
extern crate lazy_static;

pub mod api;
pub mod error;
pub mod node;
mod rpc;
pub mod state;
pub mod tcp;
mod test_support;

pub type AsyncError = Box<dyn std::error::Error + Send + Sync>;
pub type Result<T> = std::result::Result<T, AsyncError>;
pub const NEWLINE: u8 = '\n' as u8;

pub fn hash(input: &Vec<u8>) -> u64 {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::Hasher;
    let mut hasher = DefaultHasher::new();
    hasher.write(input.as_ref());
    hasher.finish()
}

#[cfg(test)]
mod hasher_tests {
    use super::*;

    #[test]
    fn hashing_is_deterministic() {
        assert_eq!(hash(&b"foo".to_vec()), hash(&b"foo".to_vec()))
    }
}
