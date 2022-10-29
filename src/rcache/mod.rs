use crate::rcache::cache_key::CacheKey;
use crate::rcache::policy::{DefaultPolicy, Policy};
use crate::rcache::ring::RingBuffer;
use crate::rcache::sharded_map::ShardedMap;
use crate::rcache::store::Store;
use crate::Error;
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::{Hash, Hasher};
use std::marker::PhantomData;
use std::ops::Add;
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
    value: V,
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

impl<V> Default for Config<V>
where
    V: Clone,
{
    fn default() -> Self {
        Self {
            on_evict: None,
            on_reject: None,
            num_counters: 1e7 as u64,
            max_cost: 1 << 20,
            pool_capacity: 30,
            get_buffer_size: 64,
            set_buffer_size: 32 * 1024,
        }
    }
}

pub(crate) struct Config<V>
where
    V: Clone,
{
    pub(crate) on_evict: ItemCallBackFn<V>,
    pub(crate) on_reject: ItemCallBackFn<V>,
    pub(crate) num_counters: u64,
    pub(crate) max_cost: i64,
    pub(crate) pool_capacity: usize,
    pub(crate) get_buffer_size: usize,
    pub(crate) set_buffer_size: usize,
}

struct InnerCache<V: Clone> {
    store: Arc<ShardedMap<V>>,
    policy: Arc<DefaultPolicy>,
    config: Arc<Config<V>>,
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
    V: Clone + Debug + Send + Sync + 'static,
{
    pub(crate) fn new() -> Cache<K, V> {
        Cache::<K, V>::with_config(Config::default(), ZeroCost)
    }

    pub(crate) fn with_config(config: Config<V>, cost: impl Cost<V> + Sized + 'static) -> Self {
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

    pub(crate) fn insert(&self, key: K, value: V, ttl: Duration) -> crate::Result<()> {
        let (key, conflict) = key.key_to_hash();
        let cost = self.cost.cost(&value);
        let mut exp = SystemTime::now();
        exp.add(ttl);
        let mut entry = Entry {
            flag: EntryFlag::New,
            key,
            conflict,
            value,
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
    std::thread::spawn(move || loop {
        let is_closed = close.load(Ordering::Acquire);
        if is_closed {
            println!("Closing");
            break;
        }

        if let Ok(entry) = set_buffer_handler.try_recv() {
            println!("Received {:?}", entry);
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
                        // Log metrics
                    }
                }
                EntryFlag::Update => cache.policy.update(entry.key, entry.cost),
            }
        }
    })
}

#[cfg(test)]
mod test {
    use crate::rcache::{Cache, ZeroCost};
    use std::time::Duration;

    fn wait() {
        std::thread::sleep(Duration::from_millis(10))
    }

    #[test]
    fn basic() {
        let cache = Cache::new();
        cache.insert(3, 40, Duration::from_secs(0)).unwrap();
        wait();
        let cache = Cache::new();
        cache.insert(b"aba", 40, Duration::from_secs(0)).unwrap();
        wait();
    }
}
