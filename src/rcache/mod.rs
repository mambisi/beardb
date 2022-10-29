use crate::rcache::cache_key::CacheKey;
use crate::rcache::policy::{DefaultPolicy, Policy};
use crate::rcache::ring::RingBuffer;
use crate::rcache::sharded_map::ShardedMap;
use crate::rcache::store::Store;
use crate::rcache::ttl::BUCKET_DURATION_SECS;
use crate::Error;
use crossbeam::channel::tick;
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::{Hash, Hasher};
use std::marker::PhantomData;
use std::ops::{Add, Div};
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, SyncSender};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant, SystemTime};
use xxhash_rust::const_xxh64::xxh64;
use xxhash_rust::xxh3::Xxh3;

mod bloom;
mod cache_key;
mod cm_sketch;
mod policy;
mod pool;
mod ring;
mod sharded_map;
mod store;
mod ttl;
mod utils;

// TODO: make it configurable
const SET_BUF_SIZE: usize = 32 * 1024;

#[derive(Debug)]
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

#[derive(Debug)]
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
    fn cost(&self, v: &V) -> i64 {
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
        }
    }
}

pub(crate) struct Config {
    pub(crate) num_counters: u64,
    pub(crate) max_cost: i64,
    pub(crate) pool_capacity: usize,
    pub(crate) get_buffer_size: usize,
    pub(crate) set_buffer_size: usize,
    pub(crate) ignore_internal_cost: bool,
}

struct InnerCache<V: Clone> {
    store: Arc<ShardedMap<V>>,
    policy: Arc<DefaultPolicy>,
    config: Arc<Config>,
}

struct Cache<K, V: Clone> {
    inner: Arc<InnerCache<V>>,
    set_buf: SyncSender<Entry<V>>,
    get_buf: Arc<RingBuffer>,
    closed: Arc<AtomicBool>,
    hasher: DefaultTwoHasher<K>,
    processor_thread: JoinHandle<()>,
    cost: Arc<dyn Cost<V>>,
}

impl<K, V: Clone> Drop for Cache<K, V> {
    fn drop(&mut self) {
        self.closed.store(true, Ordering::Release);
    }
}

impl<K, V> Cache<K, V>
where
    K: CacheKey,
    V: Clone + Debug + Send + Sync + Default + 'static,
{
    pub(crate) fn new() -> Cache<K, V> {
        Cache::<K, V>::with_config(Config::default(), ZeroCost)
    }

    pub(crate) fn with_config(config: Config, cost: impl Cost<V> + Sized + 'static) -> Self {
        let closed = Arc::new(AtomicBool::new(false));
        let config = Arc::new(config);
        let store = Arc::new(ShardedMap::new());
        let policy = Arc::new(DefaultPolicy::new(config.num_counters, config.max_cost));
        let get_buffer = Arc::new(RingBuffer::new(
            policy.clone(),
            config.pool_capacity,
            config.get_buffer_size,
        ));
        let (set_buffer, set_buffer_handler) =
            std::sync::mpsc::sync_channel(config.set_buffer_size);

        let inner = Arc::new(InnerCache {
            store,
            policy,
            config,
        });

        let op = process_items(closed.clone(), set_buffer_handler, inner.clone());
        Self {
            inner,
            set_buf: set_buffer,
            get_buf: get_buffer,
            closed,
            hasher: DefaultTwoHasher(PhantomData::default()),
            processor_thread: op,
            cost: Arc::new(cost),
        }
    }

    pub(crate) fn insert(&self, key: K, value: V) -> crate::Result<()> {
        self.insert_with_ttl(key, value, Duration::from_secs(0))
    }

    pub(crate) fn get(&self, key: K) -> Option<V> {
        if self.closed.load(Ordering::Acquire) {
            return None;
        }
        let (key, conflict) = key.key_to_hash();
        self.get_buf.push(key);
        let value = self.inner.store.get(key, conflict);
        if value.is_some() {
            // TODO: add Metrics
        } else {
            // TODO: add Metrics
        }
        value
    }

    pub(crate) fn remove(&self, key: K) -> crate::Result<()> {
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
            .map_err(|e| Error::AnyError(Box::new(e)))
    }

    pub(crate) fn insert_with_ttl(&self, key: K, value: V, ttl: Duration) -> crate::Result<()> {
        let (key, conflict) = key.key_to_hash();
        let mut cost = self.cost.cost(&value);
        if !self.inner.config.ignore_internal_cost {
            cost += std::mem::size_of::<Entry<V>>() as i64;
        }
        let mut exp = if ttl.as_secs() > 0 {
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
        self.set_buf
            .send(entry)
            .map_err(|e| Error::AnyError(Box::new(e)))
    }
}

fn process_items<V: Clone + Debug + Send + Sync + 'static>(
    close: Arc<AtomicBool>,
    set_buffer_handler: Receiver<Entry<V>>,
    cache: Arc<InnerCache<V>>,
) -> JoinHandle<()> {
    let ticker = tick(Duration::from_secs(BUCKET_DURATION_SECS as u64).div(2));
    let on_evict = |e: &Entry<V>| {};
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
                        cache.store.set(entry)
                    }
                    if let Some(victims) = victims {
                        for victim in victims {
                            cache.store.remove(victim.key, 0);
                        }
                    }
                }
                EntryFlag::Delete => {
                    cache.policy.remove(&entry.key);
                    if let Some((_, val)) = cache.store.remove(entry.key, entry.conflict) {
                        //TODO: Log metrics
                    }
                }
                EntryFlag::Update => cache.policy.update(entry.key, entry.cost),
            }
        }

        if let Ok(tick) = ticker.try_recv() {
            cache.store.cleanup(cache.policy.clone());
        }
    })
}

#[cfg(test)]
mod test {
    use crate::rcache::{Cache, Config, Entry, ZeroCost};
    use std::default::default;
    use std::fmt::Debug;
    use std::sync::Arc;
    use std::time::Duration;

    fn wait(millis: u64) {
        std::thread::sleep(Duration::from_millis(millis))
    }

    #[test]
    fn basic() {
        let cache = Cache::with_config(Config::default(), ZeroCost);
        cache.insert(3, 40).unwrap();
        wait(10);
        let cache = Cache::new();
        cache.insert(b"aba", 40).unwrap();
        wait(10);
        cache
            .insert_with_ttl(b"bcd", 90, Duration::from_secs(2))
            .unwrap();
        wait(10);
        println!("{:?}", cache.get(b"bcd"));
        wait(5000);
        println!("{:?}", cache.get(b"bcd"));
    }
}
