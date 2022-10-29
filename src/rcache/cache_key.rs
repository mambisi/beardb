use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use xxhash_rust::xxh3::Xxh3;

pub(crate) trait CacheKey {
    fn key_to_hash(&self) -> (u64, u64);
}

impl CacheKey for i8 {
    #[inline]
    fn key_to_hash(&self) -> (u64, u64) {
        (*self as u64, 0)
    }
}

impl CacheKey for u8 {
    #[inline]
    fn key_to_hash(&self) -> (u64, u64) {
        (*self as u64, 0)
    }
}

impl CacheKey for i16 {
    #[inline]
    fn key_to_hash(&self) -> (u64, u64) {
        (*self as u64, 0)
    }
}

impl CacheKey for u16 {
    #[inline]
    fn key_to_hash(&self) -> (u64, u64) {
        (*self as u64, 0)
    }
}

impl CacheKey for i32 {
    fn key_to_hash(&self) -> (u64, u64) {
        (*self as u64, 0)
    }
}

impl CacheKey for u32 {
    #[inline]
    fn key_to_hash(&self) -> (u64, u64) {
        (*self as u64, 0)
    }
}

impl CacheKey for i64 {
    #[inline]
    fn key_to_hash(&self) -> (u64, u64) {
        (*self as u64, 0)
    }
}

impl CacheKey for u64 {
    #[inline]
    fn key_to_hash(&self) -> (u64, u64) {
        (*self as u64, 0)
    }
}

impl<T: ?Sized + Hash> CacheKey for &T {
    #[inline]
    fn key_to_hash(&self) -> (u64, u64) {
        let mut default_hasher = DefaultHasher::new();
        self.hash(&mut default_hasher);
        let mut xxhasher = Xxh3::new();
        self.hash(&mut xxhasher);
        (default_hasher.finish(), xxhasher.finish())
    }
}
