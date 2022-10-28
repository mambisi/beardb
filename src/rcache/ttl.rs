use crate::rcache::policy::Policy;
use crate::rcache::store::{Store};
use crate::rcache::utils::{is_time_zero, utc_zero};
use crate::rcache::{Entry, EntryFlag, ItemCallBackFn};
use chrono::{DateTime, Utc};
use parking_lot::{RwLock, RwLockWriteGuard};
use std::collections::HashMap;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::rc::Rc;
use std::time::{Instant, SystemTime};

const BUCKET_DURATION_SECS: i64 = 5;

type Bucket = HashMap<u64, u64>;

pub(crate) fn storage_bucket(t: DateTime<Utc>) -> i64 {
    (t.timestamp() / BUCKET_DURATION_SECS) + 1
}

pub(crate) fn clean_bucket(t: DateTime<Utc>) -> i64 {
    storage_bucket(t) - 1
}

#[derive(Debug, Default)]
pub(crate) struct ExpirationMap<V> {
    pub(crate) buckets: RwLock<HashMap<i64, Bucket>>,
    marker_: PhantomData<V>,
}

impl<V> ExpirationMap<V>
{
    pub(crate) fn new() -> Self {
        Self {
            buckets: Default::default(),
            marker_: Default::default(),
        }
    }

    pub(crate) fn add(&self, key: u64, conflict: u64, exp: DateTime<Utc>) {
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
        exp: DateTime<Utc>,
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
        exp: DateTime<Utc>,
    ) {
        buckets.entry(storage_bucket(exp)).and_modify(|bucket| {
            bucket.remove(&key);
        });
    }

    pub(crate) fn update(
        &self,
        key: u64,
        conflict: u64,
        old_exp: DateTime<Utc>,
        new_exp: DateTime<Utc>,
    ) {
        let mut buckets = self.buckets.write();
        self.remove_(&mut buckets, key, old_exp);
        self.add_(&mut buckets, key, conflict, new_exp);
    }

    pub(crate) fn remove(&self, key: u64, exp: DateTime<Utc>) {
        let mut buckets = self.buckets.write();
        self.remove_(&mut buckets, key, exp);
    }
}
