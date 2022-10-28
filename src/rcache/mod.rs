use crate::rcache::policy::{DefaultPolicy, Policy};
use crate::rcache::ring::RingBuffer;
use crate::rcache::sharded_map::ShardedMap;
use crate::rcache::store::Store;
use chrono::{DateTime, Utc};
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::{Hash, Hasher};
use std::marker::PhantomData;
use std::ops::Add;
use std::rc::Rc;
use std::sync::Arc;
use std::time::{Duration, Instant};
use xxhash_rust::const_xxh64::xxh64;
use xxhash_rust::xxh3::Xxh3;

mod bloom;
mod cm_sketch;
mod policy;
mod pool;
mod ring;
mod sharded_map;
mod store;
mod ttl;
mod utils;

pub(crate) trait CacheItem: Send + Clone {}

#[derive(Debug)]
pub(crate) enum EntryFlag {
    New,
    Delete,
    Update,
}

#[derive(Debug)]
pub(crate) struct Entry<V>
where
    V: CacheItem,
{
    flag: EntryFlag,
    key: u64,
    conflict: u64,
    value: V,
    cost: i64,
    exp: DateTime<Utc>,
}
type ItemCallBackFn<V> = Arc<Option<Box<dyn 'static + Send + Sync + ItemCallback<V>>>>;
pub(crate) trait ItemCallback<V>: Fn(&Entry<V>) {}

impl<F, V> ItemCallback<V> for F where F: Fn(&Entry<V>) {}

pub(crate) struct Config<V>
where
    V: CacheItem,
{
    on_evict: ItemCallBackFn<V>,
    on_reject: ItemCallBackFn<V>,
}

fn hash<K>(key: K) -> (u64, u64)
where
    K: Hash,
{
    let mut default_hasher = DefaultHasher::new();
    key.hash(&mut default_hasher);
    let mut xxhasher = Xxh3::new();
    key.hash(&mut xxhasher);
    (default_hasher.finish(), xxhasher.finish())
}

pub(crate) struct Cache<K, V: CacheItem> {
    store: Arc<ShardedMap<V>>,
    policy: Arc<DefaultPolicy>,
    get_buf: Arc<RingBuffer>,
    marker_: PhantomData<K>,
}

impl<K, V> Cache<K, V>
where
    K: Clone + Hash,
    V: CacheItem,
{
    fn new() -> Self {
        let store = Arc::new(ShardedMap::new());
        let policy = Arc::new(DefaultPolicy::new(1e7 as u64, 1 << 20));
        let ring = Arc::new(RingBuffer::new(policy.clone(), 30, 64));
        Self {
            store,
            policy,
            get_buf: ring,
            marker_: Default::default(),
        }
    }
}
