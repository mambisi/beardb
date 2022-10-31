#![feature(default_free_fn)]

use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::{Hash, Hasher};
use std::marker::PhantomData;
use std::ops::{Add, Div};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, SyncSender};
use std::time::{Duration, Instant, SystemTime};

use crossbeam::channel::tick;
use parking_lot::RwLock;
use xxhash_rust::xxh3::Xxh3;

use crate::cache_key::CacheKey;
use crate::error::Error;
use crate::metrics::{Metrics, MetricType};
use crate::policy::{DefaultPolicy, Policy};
use crate::ring::RingBuffer;
use crate::sharded_map::ShardedMap;
use crate::store::Store;
use crate::ttl::BUCKET_DURATION_SECS;

mod bloom;
mod cache_key;
mod cm_sketch;
mod error;
mod metrics;
mod policy;
mod pool;
mod ring;
mod sharded_map;
mod store;
mod ttl;
mod utils;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Copy, Clone, Ord, PartialOrd, Eq, PartialEq)]
pub(crate) enum EntryFlag {
    New,
    Delete,
    Update,
}

pub(crate) trait Cost<V>
where
    V: Clone,
{
    fn cost(&self, v: &V) -> i64;
}

pub(crate) trait TwoHash64<K>
where
    K: Hash,
{
    fn hash(&self, key: &K) -> (u64, u64);
}

#[derive(Debug, Clone)]
pub(crate) struct Entry<V>
    where
        V: Clone,
{
    flag: EntryFlag,
    key: u64,
    conflict: u64,
    value: Option<V>,
    cost: i64,
    exp: SystemTime,
}

#[derive(Debug, Clone)]
pub struct PartialEntry<V>
    where
        V: Clone,
{
    pub key: u64,
    pub conflict: u64,
    pub value: Option<V>,
    pub cost: i64,
}

pub trait Handler<V: Clone>: Send + Sync {
    fn on_evict(&self, entry: PartialEntry<V>);
    fn on_reject(&self, entry: PartialEntry<V>);
    fn on_exit(&self, entry: V);
}

struct DefaultTwoHasher<K>(PhantomData<K>);

impl<K> TwoHash64<K> for DefaultTwoHasher<K>
where
    K: Hash,
{
    fn hash(&self, key: &K) -> (u64, u64) {
        let mut default_hasher = DefaultHasher::new();
        key.hash(&mut default_hasher);
        let mut xxhasher = Xxh3::new();
        key.hash(&mut xxhasher);
        (default_hasher.finish(), xxhasher.finish())
    }
}

struct ZeroCost;
impl<V> Cost<V> for ZeroCost
where
    V: Clone,
{
    fn cost(&self, _v: &V) -> i64 {
        0
    }
}

impl<V> Default for Config<V>
    where
        V: Clone,
{
    fn default() -> Self {
        Self {
            handler: None,
            num_counters: 1e7 as u64,
            max_cost: 1 << 20,
            pool_capacity: 30,
            get_buffer_size: 64,
            set_buffer_size: 32 * 1024,
            ignore_internal_cost: false,
            enable_metrics: true,
        }
    }
}

pub struct Config<V>
    where
        V: Clone,
{
    pub handler: Option<Box<dyn Handler<V>>>,
    pub num_counters: u64,
    pub max_cost: i64,
    pub pool_capacity: usize,
    pub get_buffer_size: usize,
    pub set_buffer_size: usize,
    pub ignore_internal_cost: bool,
    pub enable_metrics: bool,
}

#[derive(Clone)]
struct InnerCache<V: Clone> {
    store: Arc<ShardedMap<V>>,
    policy: Arc<DefaultPolicy>,
    config: Arc<Config<V>>,
    metrics: Option<Metrics>,
}

unsafe impl<V: Send + Sync + Clone> Send for InnerCache<V> {}

unsafe impl<V: Send + Sync + Clone> Sync for InnerCache<V> {}

impl<V: Send + Sync + Clone> Handler<V> for InnerCache<V> {
    fn on_evict(&self, entry: PartialEntry<V>) {
        if let Some(handler) = self.config.handler.as_ref() {
            handler.on_evict(entry.clone());
            if let Some(value) = entry.value {
                self.on_exit(value);
            }
        }
    }

    fn on_reject(&self, entry: PartialEntry<V>) {
        if let Some(handler) = self.config.handler.as_ref() {
            handler.on_reject(entry.clone());
            if let Some(value) = entry.value {
                self.on_exit(value);
            }
        }
    }

    fn on_exit(&self, entry: V) {
        if let Some(handler) = self.config.handler.as_ref() {
            handler.on_exit(entry);
        }
    }
}

struct Cache<K, V: Clone> {
    inner: InnerCache<V>,
    set_buf: SyncSender<Entry<V>>,
    get_buf: RingBuffer,
    closed: Arc<AtomicBool>,
    hasher: DefaultTwoHasher<K>,
    cost: Arc<dyn Cost<V>>,
}

impl<K, V: Clone> Drop for Cache<K, V> {
    fn drop(&mut self) {
        self.closed.store(true, Ordering::Release);
    }
}

unsafe impl<K, V: Send + Sync + Clone> Send for Cache<K, V> {}

unsafe impl<K, V: Send + Sync + Clone> Sync for Cache<K, V> {}

impl<K, V> Cache<K, V>
    where
        K: CacheKey,
        V: Clone + Debug + Send + Sync + Default + 'static,
{
    pub fn new() -> Cache<K, V> {
        Cache::<K, V>::with_config(Config::default(), ZeroCost)
    }

    pub fn with_config(config: Config<V>, cost: impl Cost<V> + Sized + 'static) -> Self {
        let metrics = if config.enable_metrics {
            Some(Metrics::new())
        } else {
            None
        };
        let closed = Arc::new(AtomicBool::new(false));
        let config = Arc::new(config);
        let store = Arc::new(ShardedMap::new());
        let policy = Arc::new(DefaultPolicy::new_with_metrics(
            config.num_counters,
            config.max_cost,
            metrics.clone(),
        ));
        let get_buf = RingBuffer::new(policy.clone(), config.pool_capacity, config.get_buffer_size);
        let (set_buf, set_buffer_handler) = std::sync::mpsc::sync_channel(config.set_buffer_size);

        let inner = InnerCache {
            store,
            policy,
            config,
            metrics,
        };

        process_items(closed.clone(), set_buffer_handler, inner.clone());

        Self {
            inner,
            set_buf,
            get_buf,
            closed,
            hasher: DefaultTwoHasher(PhantomData::default()),
            cost: Arc::new(cost),
        }
    }

    pub fn insert(&self, key: K, value: V) -> Result<()> {
        self.insert_with_ttl(key, value, Duration::from_secs(0))
    }

    pub fn get(&self, key: K) -> Option<V> {
        if self.closed.load(Ordering::Acquire) {
            return None;
        }
        let (key, conflict) = key.key_to_hash();
        self.get_buf.push(key);
        let value = self.inner.store.get(key, conflict);
        if let Some(metrics) = self.inner.metrics.as_ref() {
            if value.is_some() {
                metrics.add(MetricType::Hit, key, 1)
            } else {
                metrics.add(MetricType::Miss, key, 1)
            }
        }
        value
    }

    pub fn remove(&self, key: K) -> Result<()> {
        if self.closed.load(Ordering::Acquire) {
            return Ok(());
        }
        let (key, conflict) = key.key_to_hash();
        self.get_buf.push(key);
        let _ = self.inner.store.remove(key, conflict);
        self.set_buf
            .send(Entry {
                flag: EntryFlag::Delete,
                key,
                conflict,
                value: None,
                cost: 0,
                exp: SystemTime::UNIX_EPOCH,
            })
            .map_err(|e| Error::SendError(Box::new(e)))
    }

    pub fn insert_with_ttl(&self, key: K, value: V, ttl: Duration) -> Result<()> {
        let (key, conflict) = key.key_to_hash();
        let mut cost = self.cost.cost(&value);
        if !self.inner.config.ignore_internal_cost {
            cost += std::mem::size_of::<Entry<V>>() as i64;
        }
        let exp = if ttl.as_secs() > 0 {
            SystemTime::now().add(ttl)
        } else {
            SystemTime::UNIX_EPOCH
        };

        let mut entry = Entry {
            flag: EntryFlag::New,
            key,
            conflict,
            value: Some(value),
            cost,
            exp,
        };

        if self.inner.store.update(&entry).is_some() {
            entry.flag = EntryFlag::Update;
        }
        let flag = entry.flag;
        let send_results = self.set_buf.send(entry);
        if send_results.is_err() {
            if flag == EntryFlag::Update {
                return Ok(());
            }
            if let Some(metrics) = self.inner.metrics.as_ref() {
                metrics.add(MetricType::DropSets, key, 1);
            }
        }
        send_results.map_err(|e| Error::SendError(Box::new(e)))
    }
    pub fn metrics(&self) -> Option<&Metrics> {
        self.inner.metrics.as_ref()
    }
}

struct ProcessItemsHandler<V: Clone> {
    start_tx: Arc<RwLock<HashMap<u64, Instant>>>,
    cache: InnerCache<V>,
    num_to_keep: usize,
}

impl<V: Clone> ProcessItemsHandler<V> {
    fn track_admission(&self, key: u64) {
        let mut start_tx = self.start_tx.write();
        if self.cache.metrics.is_some() {
            start_tx.insert(key, Instant::now());
            if start_tx.len() > self.num_to_keep {
                let _ = start_tx.drain().take(self.num_to_keep);
            }
        }
    }
}

impl<V: Clone + Send + Sync> Handler<V> for ProcessItemsHandler<V> {
    fn on_evict(&self, entry: PartialEntry<V>) {
        let mut start_tx = self.start_tx.write();
        if let Some(tx) = start_tx.remove(&entry.key) {
            if let Some(metrics) = self.cache.metrics.as_ref() {
                metrics.track_eviction(tx.elapsed().as_secs());
            }
        }
        self.cache.on_evict(entry)
    }

    fn on_reject(&self, entry: PartialEntry<V>) {
        self.cache.on_reject(entry)
    }

    fn on_exit(&self, entry: V) {
        self.cache.on_exit(entry)
    }
}

fn process_items<V: Clone + Debug + Send + Sync + 'static>(
    close: Arc<AtomicBool>,
    set_buffer_handler: Receiver<Entry<V>>,
    cache: InnerCache<V>,
) {
    let ticker = tick(Duration::from_secs(BUCKET_DURATION_SECS.div(2) as u64));
    let handler = ProcessItemsHandler {
        start_tx: Arc::new(Default::default()),
        cache: cache.clone(),
        num_to_keep: 100000,
    };
    let mut last_tick = Instant::now();
    std::thread::spawn(move || loop {
        let is_closed = close.load(Ordering::Acquire);
        if is_closed {
            break;
        }

        if let Ok(entry) = set_buffer_handler.try_recv() {
            match entry.flag {
                EntryFlag::New => {
                    let (victims, added) = cache.policy.add(entry.key, entry.cost);
                    if added {
                        let key = entry.key;
                        cache.store.set(entry);
                        if let Some(metrics) = &cache.metrics {
                            metrics.add(MetricType::KeyAdd, key, 1);
                            handler.track_admission(key)
                        }
                    }
                    if let Some(victims) = victims {
                        for victim in victims {
                            let mut entry = PartialEntry {
                                key: victim.key,
                                conflict: victim.conflict,
                                value: None,
                                cost: victim.cost,
                            };
                            let (conflict, value) = cache.store.remove(victim.key, 0);
                            entry.conflict = conflict;
                            entry.value = value;
                            handler.on_evict(entry)
                        }
                    }
                }
                EntryFlag::Delete => {
                    cache.policy.remove(&entry.key);
                    let (_, value) = cache.store.remove(entry.key, entry.conflict);
                    if let Some(value) = value {
                        cache.on_exit(value);
                    }
                }
                EntryFlag::Update => cache.policy.update(entry.key, entry.cost),
            }
        }

        if let Ok(tick) = ticker.try_recv() {
            println!("TICK {:?}", tick.duration_since(last_tick).as_millis());
            last_tick = tick;
            cache.store.cleanup(cache.policy.as_ref(), &handler);
        }
    });
}

#[cfg(test)]
mod test {
    use std::default::default;
    use std::fmt::Debug;
    use std::sync::Arc;
    use std::time::Duration;

    use crate::{Cache, Config, Handler, PartialEntry, ZeroCost};

    fn wait(millis: u64) {
        std::thread::sleep(Duration::from_millis(millis))
    }

    struct ItemsHandler;

    impl<V: Clone + Send + Sync + Debug> Handler<V> for ItemsHandler {
        fn on_evict(&self, entry: PartialEntry<V>) {
            println!("ItemsHandler on_evict {:?}", entry)
        }

        fn on_reject(&self, entry: PartialEntry<V>) {
            println!("ItemsHandler on_reject {:?}", entry)
        }

        fn on_exit(&self, entry: V) {
            println!("ItemsHandler on_exit {:?}", entry)
        }
    }

    #[test]
    fn basic() {
        let cache = Cache::with_config(
            Config {
                handler: Some(Box::new(ItemsHandler)),
                ..default()
            },
            ZeroCost,
        );
        assert!(cache.insert(b"aba", 40).is_ok());
        wait(10);
        assert!(cache
            .insert_with_ttl(b"bcd", 90, Duration::from_secs(2))
            .is_ok());
        wait(10);
        println!("{:?}", cache.get(b"bcd"));
        wait(5000);
        println!("{:?}", cache.get(b"bcd"));
        wait(2000);
        println!("{}", cache.metrics().unwrap());
    }
}
