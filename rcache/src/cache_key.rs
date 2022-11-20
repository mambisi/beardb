use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use xxhash_rust::xxh3::Xxh3;

pub trait HashableKey {
    fn key_to_hash(&self) -> (u64, u64);
}

impl HashableKey for i8 {
    #[inline]
    fn key_to_hash(&self) -> (u64, u64) {
        (*self as u64, 0)
    }
}

impl HashableKey for u8 {
    #[inline]
    fn key_to_hash(&self) -> (u64, u64) {
        (*self as u64, 0)
    }
}

impl HashableKey for i16 {
    #[inline]
    fn key_to_hash(&self) -> (u64, u64) {
        (*self as u64, 0)
    }
}

impl HashableKey for u16 {
    #[inline]
    fn key_to_hash(&self) -> (u64, u64) {
        (*self as u64, 0)
    }
}

impl HashableKey for i32 {
    fn key_to_hash(&self) -> (u64, u64) {
        (*self as u64, 0)
    }
}

impl HashableKey for u32 {
    #[inline]
    fn key_to_hash(&self) -> (u64, u64) {
        (*self as u64, 0)
    }
}

impl HashableKey for i64 {
    #[inline]
    fn key_to_hash(&self) -> (u64, u64) {
        (*self as u64, 0)
    }
}

impl HashableKey for u64 {
    #[inline]
    fn key_to_hash(&self) -> (u64, u64) {
        (*self as u64, 0)
    }
}

impl<T: ?Sized + Hash> HashableKey for &T {
    #[inline]
    fn key_to_hash(&self) -> (u64, u64) {
        let mut default_hasher = DefaultHasher::new();
        self.hash(&mut default_hasher);
        let mut xxhasher = Xxh3::new();
        self.hash(&mut xxhasher);
        (default_hasher.finish(), xxhasher.finish())
    }
}


impl<T: ?Sized + Hash> HashableKey for &mut T {
    #[inline]
    fn key_to_hash(&self) -> (u64, u64) {
        let mut default_hasher = DefaultHasher::new();
        self.hash(&mut default_hasher);
        let mut xxhasher = Xxh3::new();
        self.hash(&mut xxhasher);
        (default_hasher.finish(), xxhasher.finish())
    }
}
