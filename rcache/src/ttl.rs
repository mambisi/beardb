use std::collections::HashMap;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::time::{SystemTime, UNIX_EPOCH};

use parking_lot::{RwLock, RwLockWriteGuard};

use crate::utils::is_time_zero;

// TODO: make it configurable.
pub(crate) const BUCKET_DURATION_SECS: i64 = 5;

type Bucket = HashMap<u64, u64>;

pub(crate) fn storage_bucket(t: SystemTime) -> i64 {
    let timestamp = t.duration_since(UNIX_EPOCH).unwrap();
    (timestamp.as_secs() as i64 / BUCKET_DURATION_SECS) + 1
}

pub(crate) fn clean_bucket(t: SystemTime) -> i64 {
    storage_bucket(t) - 1
}

#[derive(Debug, Default)]
pub(crate) struct ExpirationMap<V> {
    pub(crate) buckets: RwLock<HashMap<i64, Bucket>>,
    marker_: PhantomData<V>,
}

impl<V> ExpirationMap<V> {
    pub(crate) fn new() -> Self {
        Self {
            buckets: Default::default(),
            marker_: Default::default(),
        }
    }

    pub(crate) fn add(&self, key: u64, conflict: u64, exp: SystemTime) {
        if is_time_zero(&exp) {
            return;
        }
        let mut buckets = self.buckets.write();
        self.add_(&mut buckets, key, conflict, exp);
    }

    fn add_(
        &self,
        buckets: &mut RwLockWriteGuard<HashMap<i64, Bucket>>,
        key: u64,
        conflict: u64,
        exp: SystemTime,
    ) {
        buckets
            .entry(storage_bucket(exp))
            .and_modify(|bucket| {
                bucket.insert(key, conflict);
            })
            .or_insert_with(|| {
                let mut bucket = HashMap::new();
                bucket.insert(key, conflict);
                bucket
            });
    }

    fn remove_(
        &self,
        buckets: &mut RwLockWriteGuard<HashMap<i64, Bucket>>,
        key: u64,
        exp: SystemTime,
    ) {
        buckets.entry(storage_bucket(exp)).and_modify(|bucket| {
            bucket.remove(&key);
        });
    }

    pub(crate) fn update(&self, key: u64, conflict: u64, old_exp: SystemTime, new_exp: SystemTime) {
        let mut buckets = self.buckets.write();
        self.remove_(&mut buckets, key, old_exp);
        self.add_(&mut buckets, key, conflict, new_exp);
    }

    pub(crate) fn remove(&self, key: u64, exp: SystemTime) {
        let mut buckets = self.buckets.write();
        self.remove_(&mut buckets, key, exp);
    }
}
