use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;
use std::time::SystemTime;

use arrayvec::ArrayVec;
use parking_lot::RwLock;

use crate::{Broadcast, Entry, Event, ItemCallBackFn, PartialEntry};
use crate::policy::Policy;
use crate::store::Store;
use crate::ttl::{clean_bucket, ExpirationMap};
use crate::utils::{is_time_zero, utc_zero};

const NUM_SHARDS: u64 = 256;

pub(crate) struct ShardedMap<V: Clone> {
    shards: ArrayVec<LockedMap<V>, { NUM_SHARDS as usize }>,
    em: Arc<ExpirationMap<V>>,
}

impl<V> ShardedMap<V>
    where
    V: Clone,
{
    pub(crate) fn new() -> Self {
        let em = Arc::new(ExpirationMap::new());
        let mut shards: ArrayVec<LockedMap<V>, { NUM_SHARDS as usize }> = ArrayVec::new_const();
        for _ in 0..NUM_SHARDS {
            shards.push(LockedMap::new(em.clone()))
        }
        Self { shards, em }
    }
}

impl<V> Store<V> for ShardedMap<V>
where
    V: Clone,
{
    fn get(&self, key: u64, conflict: u64) -> Option<V> {
        self.shards[(key % NUM_SHARDS) as usize].get(key, conflict)
    }

    fn set(&self, entry: Entry<V>) {
        self.shards[(entry.key % NUM_SHARDS) as usize].set(entry)
    }

    fn expiration(&self, key: u64) -> SystemTime {
        self.shards[(key % NUM_SHARDS) as usize].expiration(key)
    }

    fn remove(&self, key: u64, conflict: u64) -> Option<(u64, V)> {
        self.shards[(key % NUM_SHARDS) as usize].remove(key, conflict)
    }

    fn update(&self, entry: &Entry<V>) -> Option<V> {
        self.shards[(entry.key % NUM_SHARDS) as usize].update(&entry)
    }

    fn cleanup(&self, policy: &dyn Policy) {
        let mut buckets = self.em.buckets.write();
        let now = SystemTime::now();
        let bucket_id = clean_bucket(now);
        let bucket = match buckets.remove(&bucket_id) {
            None => {
                return;
            }
            Some(bucket) => bucket,
        };
        drop(buckets);
        for (key, conflict) in bucket {
            if self.expiration(key) > now {
                continue;
            }

            let _ = policy.cost(&key);
            policy.remove(&key);
            let _ = self.remove(key, conflict);
        }
    }

    fn clear(&self, callback: ItemCallBackFn<V>) {
        for i in 0..NUM_SHARDS {
            self.shards[i as usize].clear(callback.clone())
        }
    }
}

#[derive(Debug)]
struct LockedMap<V: Clone> {
    data: RwLock<HashMap<u64, Entry<V>>>,
    em: Arc<ExpirationMap<V>>,
}

impl<V> LockedMap<V>
where
    V: Clone,
{
    fn new(em: Arc<ExpirationMap<V>>) -> Self {
        Self {
            data: Default::default(),
            em,
        }
    }

    fn set(&self, entry: Entry<V>) {
        let mut data = self.data.write();
        if let Some(e) = data.get(&entry.key) {
            if entry.conflict != 0 && (entry.conflict != e.conflict) {
                return;
            }
        } else {
            self.em.add(entry.key, entry.conflict, entry.exp)
        }

        data.insert(entry.key, entry);
    }

    fn get(&self, key: u64, conflict: u64) -> Option<V> {
        let data = self.data.read();
        let entry = data.get(&key)?;

        if conflict != 0 && (conflict != entry.conflict) {
            return None;
        }

        let now = SystemTime::now();
        if now > entry.exp {
            return None;
        }
        entry.value.clone()
    }

    fn expiration(&self, key: u64) -> SystemTime {
        let data = self.data.read();
        data.get(&key).map(|data| data.exp).unwrap_or_else(utc_zero)
    }

    fn remove(&self, key: u64, conflict: u64) -> Option<(u64, V)> {
        let mut data = self.data.write();
        let entry = data.get(&key)?;
        if conflict != 0 && (conflict != entry.conflict) {
            return None;
        }
        if !is_time_zero(&entry.exp) {
            self.em.remove(key, entry.exp);
        }
        let entry = data.remove(&key)?;
        let value = entry.value?;
        Some((entry.conflict, value))
    }

    fn update(&self, new_entry: &Entry<V>) -> Option<V> {
        let mut data = self.data.write();
        let entry = data.get(&new_entry.key)?;
        if new_entry.conflict != 0 && (new_entry.conflict != entry.conflict) {
            return None;
        }
        self.em
            .update(new_entry.key, new_entry.conflict, entry.exp, new_entry.exp);
        let entry = data.insert(new_entry.key, new_entry.clone());
        entry.map(|v| v.value).flatten()
    }

    fn clear(&self, callback: ItemCallBackFn<V>) {
        let mut data = self.data.write();
        if let Some(on_evict) = callback.as_ref() {
            for (_, e) in data.iter() {
                on_evict(e);
            }
        }
        data.clear();
    }
}
