use bytes::Bytes;
use std::collections::hash_map::DefaultHasher;

pub struct Hasher {}

impl Hasher {
    pub fn hash(input: Bytes) -> u64 {
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
        assert_eq!(Hasher::hash("foo".into()), Hasher::hash("foo".into()))
    }
}
