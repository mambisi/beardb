use std::borrow::{Borrow, BorrowMut};
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::{Hash, Hasher};
use std::marker::PhantomData;
use std::ops::{Add, Div};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::JoinHandle;
use std::time::{Duration, Instant, SystemTime};

use crossbeam::channel::{Receiver, Sender, tick};
use parking_lot::RwLock;
use xxhash_rust::xxh3::Xxh3;

use crate::broadcast::{Broadcast, Event, Subscription};
use crate::cache_key::CacheKey;
use crate::error::Error;
use crate::metrics::{Metrics, MetricType};
use crate::policy::{DefaultPolicy, Policy};
use crate::ring::RingBuffer;
use crate::sharded_map::ShardedMap;
use crate::store::Store;
use crate::ttl::BUCKET_DURATION_SECS;

mod bloom;
mod broadcast;
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

// TODO: make it configurable
const SET_BUF_SIZE: usize = 32 * 1024;

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
    broadcast: Broadcast<V>,
}

struct Cache<K, V: Clone> {
    inner: InnerCache<V>,
    set_buf: Sender<Entry<V>>,
    get_buf: Arc<RingBuffer>,
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
        let get_buffer = Arc::new(RingBuffer::new(
            policy.clone(),
            config.pool_capacity,
            config.get_buffer_size,
        ));
        let broadcast = Broadcast::new(10);
        let (set_buffer, set_buffer_handler) = crossbeam::channel::bounded(config.set_buffer_size);

        let inner = InnerCache {
            store,
            policy,
            config,
            metrics,
            broadcast,
        };

        process_items(closed.clone(), set_buffer_handler, inner.clone());

        Self {
            inner,
            set_buf: set_buffer,
            get_buf: get_buffer,
            closed,
            hasher: DefaultTwoHasher(PhantomData::default()),
            cost: Arc::new(cost),
        }
    }

    pub fn insert(&self, key: K, value: V) -> bool {
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
        let value = self.inner.store.remove(key, conflict);
        if value.is_some() {
            // TODO: add Metrics
        } else {
            // TODO: add Metrics
        }
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

    pub fn insert_with_ttl(&self, key: K, value: V, ttl: Duration) -> bool {
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

        if let Some(prev) = self.inner.store.update(&entry) {
            self.inner.broadcast.send(
                Event::Exit,
                PartialEntry {
                    key,
                    conflict,
                    value: Some(prev),
                    cost,
                },
            );
            entry.flag = EntryFlag::Update;
        }
        let flag = entry.flag;
        if self.set_buf.send(entry).is_err() {
            if flag == EntryFlag::Update {
                return true;
            }
            if let Some(metrics) = self.inner.metrics.as_ref() {
                metrics.add(MetricType::DropSets, key, 1);
            }
            return false;
        }
        return true;
    }

    pub fn subscribe(&self, event: Event) -> Subscription<V> {
        self.inner.broadcast.subscribe(event)
    }
}

fn process_items<V: Clone + Debug + Send + Sync + 'static>(
    close: Arc<AtomicBool>,
    set_buffer_handler: Receiver<Entry<V>>,
    cache: InnerCache<V>,
) {
    let ticker = tick(Duration::from_secs(BUCKET_DURATION_SECS as u64).div(2));
    let _on_evict = |_e: &Entry<V>| {};

    let start_ts: Arc<RwLock<HashMap<u64, Instant>>> = Arc::new(RwLock::new(HashMap::new()));
    let start_ts_max = 100000_usize; // TODO: Make this configurable via options.
    let broadcast = Broadcast::new(1);
    let mut on_evict = broadcast.subscribe(Event::Evict);
    {
        let cache = cache.clone();
        let start_ts = start_ts.clone();
        std::thread::spawn(move || loop {
            if let Ok(e) = on_evict.as_ref().try_recv() {
                if let Some(ts) = start_ts.read().get(&e.key) {
                    if let Some(metrics) = cache.metrics.as_ref() {
                        metrics.track_eviction(ts.elapsed().as_secs());
                    }
                }
                println!("On Evict {:?}", e);
                cache.broadcast.send(Event::Evict, e);
            }
        });
    }

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
                            track_admission(
                                &cache,
                                start_ts.write().borrow_mut(),
                                start_ts_max,
                                key,
                            )
                        }
                    }
                    if let Some(victims) = victims {
                        for mut victim in victims {
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
                            cache.broadcast.send(Event::Evict, entry)
                        }
                    }
                }
                EntryFlag::Delete => {
                    cache.policy.remove(&entry.key);
                    if let Some((_, value)) = cache.store.remove(entry.key, entry.conflict) {
                        cache.broadcast.send(
                            Event::Exit,
                            PartialEntry {
                                key: entry.key,
                                conflict: entry.conflict,
                                value: Some(value),
                                cost: entry.cost,
                            },
                        )
                    }
                }
                EntryFlag::Update => cache.policy.update(entry.key, entry.cost),
            }
        }

        if let Ok(_tick) = ticker.try_recv() {
            cache.store.cleanup(cache.policy.clone(), &broadcast);
        }
    });
}

fn track_admission<V: Clone + Debug + Send + Sync + 'static>(
    cache: &InnerCache<V>,
    start_ts: &mut HashMap<u64, Instant>,
    start_ts_max: usize,
    key: u64,
) {
    if cache.metrics.is_none() {
        return;
    }
    start_ts.insert(key, Instant::now());
    if start_ts.len() > start_ts_max {
        for _ in start_ts.drain().take(start_ts_max) {}
    }
}

#[cfg(test)]
mod test {
    use std::time::Duration;

    use crate::{Cache, Config, Event, ZeroCost};

    fn wait(millis: u64) {
        std::thread::sleep(Duration::from_millis(millis))
    }

    #[test]
    fn basic() {
        let cache = Cache::with_config(Config::default(), ZeroCost);
        let mut on_evict = cache.subscribe(Event::Evict);
        std::thread::spawn(move || loop {
            if let Ok(e) = on_evict.as_ref().try_recv() {
                println!("test::basic On Evict {:?}", e);
            }
        });
        let mut on_exit = cache.subscribe(Event::Exit);
        std::thread::spawn(move || loop {
            if let Ok(e) = on_exit.as_ref().try_recv() {
                println!("test::basic On Exit {:?}", e);
            }
        });
        assert!(cache.insert(3, 40));
        wait(10);
        let cache = Cache::new();
        assert!(cache.insert(b"aba", 40));
        wait(10);
        assert!(cache.insert_with_ttl(b"bcd", 90, Duration::from_secs(2)));
        wait(10);
        println!("{:?}", cache.get(b"bcd"));
        wait(5000);
        println!("{:?}", cache.get(b"bcd"));
        wait(10000);
        wait(5000);
    }
}
