pub mod client;
pub mod connection;
pub mod request;
pub mod response;
pub mod server;

pub const REQUEST_BUFFER_SIZE: usize = 16;

pub struct Hasher {}

impl Hasher {
    pub fn hash(input: &Vec<u8>) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::Hasher;
        let mut hasher = DefaultHasher::new();
        hasher.write(input.as_ref());
        hasher.finish()
    }
}

#[cfg(test)]
mod hasher_tests {
    use super::*;

    #[test]
    fn hashing_is_deterministic() {
        assert_eq!(
            Hasher::hash(&b"foo".to_vec()),
            Hasher::hash(&b"foo".to_vec())
        )
    }
}
