use std::collections::hash_map::DefaultHasher;
use std::fmt::Debug;
use std::hash::{Hash, Hasher};
use std::marker::PhantomData;
use std::ops::{Add, Div};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, SyncSender};
use std::time::{Duration, SystemTime};

use crossbeam::channel::tick;
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
pub(crate) struct PartialEntry<V>
    where
        V: Clone,
{
    key: u64,
    conflict: u64,
    value: Option<V>,
    cost: i64,
}

type ItemCallBackFn<V> = Option<Arc<Box<dyn 'static + Send + Sync + ItemCallback<V>>>>;

pub(crate) trait ItemCallback<V>: Fn(&Entry<V>) {}

impl<F, V> ItemCallback<V> for F where F: Fn(&Entry<V>) {}

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

impl Default for Config {
    fn default() -> Self {
        Self {
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

pub struct Config {
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
    config: Arc<Config>,
    metrics: Option<Metrics>,
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

unsafe impl<V: Send + Sync + Clone> Send for InnerCache<V> {}

unsafe impl<V: Send + Sync + Clone> Sync for InnerCache<V> {}

impl<K, V> Cache<K, V>
    where
        K: CacheKey,
        V: Clone + Debug + Send + Sync + Default + 'static,
{
    pub fn new() -> Cache<K, V> {
        Cache::<K, V>::with_config(Config::default(), ZeroCost)
    }

    pub fn with_config(config: Config, cost: impl Cost<V> + Sized + 'static) -> Self {
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
        let get_buf = RingBuffer::new(
            policy.clone(),
            config.pool_capacity,
            config.get_buffer_size,
        );
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
}

fn process_items<V: Clone + Debug + Send + Sync + 'static>(
    close: Arc<AtomicBool>,
    set_buffer_handler: Receiver<Entry<V>>,
    cache: InnerCache<V>,
) {
    let ticker = tick(Duration::from_secs(BUCKET_DURATION_SECS.div(2) as u64));
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
                            if let Some((conflict, value)) = cache.store.remove(victim.key, 0) {
                                entry.conflict = conflict;
                                entry.value = Some(value)
                            };
                        }
                    }
                }
                EntryFlag::Delete => {
                    cache.policy.remove(&entry.key);
                    let _ = cache.store.remove(entry.key, entry.conflict);
                }
                EntryFlag::Update => cache.policy.update(entry.key, entry.cost),
            }
        }

        if let Ok(_tick) = ticker.try_recv() {
            cache.store.cleanup(cache.policy.as_ref());
        }
    });
}
#[cfg(test)]
mod test {
    use std::time::Duration;

    use crate::{Cache, Config, ZeroCost};

    fn wait(millis: u64) {
        std::thread::sleep(Duration::from_millis(millis))
    }

    #[test]
    fn basic() {
        let cache = Cache::with_config(Config::default(), ZeroCost);
        assert!(cache.insert(3, 40).is_ok());
        wait(10);
        let cache = Cache::new();
        assert!(cache.insert(b"aba", 40).is_ok());
        wait(10);
        assert!(cache.insert_with_ttl(b"bcd", 90, Duration::from_secs(2)).is_ok());
        wait(10);
        println!("{:?}", cache.get(b"bcd"));
        wait(5000);
        println!("{:?}", cache.get(b"bcd"));
    }
}