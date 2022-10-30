use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use arrayvec::ArrayVec;
use histogram::Histogram;
use parking_lot::RwLock;

const METRIC_OPTIONS: [MetricType; 11] = [
    MetricType::Hit,
    MetricType::Miss,
    MetricType::KeyAdd,
    MetricType::KeyUpdate,
    MetricType::KeyEvict,
    MetricType::CostAdd,
    MetricType::CostEvict,
    MetricType::DropSets,
    MetricType::RejectSets,
    MetricType::DropGets,
    MetricType::KeepGets,
];


const HIT: &str = "hit";
const MISS: &str = "miss";
const KEY_ADD: &str = "keys-added";
const KEY_UPDATE: &str = "keys-updated";
const KEY_EVICT: &str = "keys-evicted";
const COST_ADD: &str = "cost-added";
const COST_EVICT: &str = "cost-evicted";
const DROP_SETS: &str = "sets-dropped";
const REJECT_SETS: &str = "sets-rejected";
const DROP_GETS: &str = "gets-dropped";
const KEEP_GETS: &str = "gets-kept";

#[derive(Hash, Copy, Clone, Ord, PartialOrd, Eq, PartialEq)]
pub enum MetricType {
    Hit,
    Miss,
    KeyAdd,
    KeyUpdate,
    KeyEvict,
    CostAdd,
    CostEvict,
    DropSets,
    RejectSets,
    DropGets,
    KeepGets,
}

impl MetricType {
    pub fn as_str(&self) -> &'static str {
        match self {
            MetricType::Hit => {
                HIT
            }
            MetricType::Miss => {
                MISS
            }
            MetricType::KeyAdd => {
                KEY_ADD
            }
            MetricType::KeyUpdate => {
                KEY_UPDATE
            }
            MetricType::KeyEvict => {
                KEY_EVICT
            }
            MetricType::CostAdd => {
                COST_ADD
            }
            MetricType::CostEvict => {
                COST_EVICT
            }
            MetricType::DropSets => {
                DROP_SETS
            }
            MetricType::RejectSets => {
                REJECT_SETS
            }
            MetricType::DropGets => {
                DROP_GETS
            }
            MetricType::KeepGets => {
                KEEP_GETS
            }
        }
    }
}


#[derive(Clone)]
pub struct Metrics {
    all: Arc<HashMap<MetricType, ArrayVec<AtomicU64, 256>>>,
    life: Arc<RwLock<Histogram>>,
}

unsafe impl Send for Metrics {}

unsafe impl Sync for Metrics {}

fn new_metric_array() -> ArrayVec<AtomicU64, 256> {
    let mut arr = ArrayVec::new_const();
    for _ in 0..256 {
        arr.push(AtomicU64::default())
    }
    arr
}

impl Metrics {
    pub fn new() -> Metrics {
        let mut all = HashMap::with_capacity(METRIC_OPTIONS.len());
        for t in METRIC_OPTIONS {
            all.insert(t, new_metric_array());
        }
        Self {
            all: Arc::new(all),
            life: Arc::new(RwLock::new(Histogram::configure().max_value(2 ^ 16).build().unwrap())),
        }
    }

    pub(crate) fn add(&self, t: MetricType, hash: u64, delta: u64) {
        let val = self.all.get(&t).unwrap();
        let idx = (hash % 25) * 10;
        let _ = val[idx as usize].fetch_add(delta, Ordering::Relaxed);
    }

    pub(crate) fn get(&self, t: MetricType) -> u64 {
        let val = self.all.get(&t).unwrap();
        let mut total = 0;
        for i in val {
            total += i.load(Ordering::Relaxed);
        }
        total
    }

    pub(crate) fn ratio(&self) -> f64 {
        let hits = self.get(MetricType::Hit);
        let misses = self.get(MetricType::Miss);
        if hits == 0 && misses == 0 {
            return 0.0;
        }
        hits as f64 / misses as f64
    }

    pub(crate) fn clear(&self) {
        for i in self.all.iter().map(|(_, m)| m.iter()).flatten() {
            i.store(0, Ordering::Release)
        }
        let mut life = self.life.write();
        life.clear()
    }

    pub(crate) fn track_eviction(&self, num_seconds: u64) {
        let mut life = self.life.write();
        let _ = life.increment(num_seconds);
        life.clear()
    }
}

impl Display for Metrics {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        for m in METRIC_OPTIONS {
            writeln!(f, "{}: {} ", m.as_str(), self.get(m))?;
        }
        write!(f, "gets-total: {} ", self.get(MetricType::Hit) + self.get(MetricType::Miss))?;
        write!(f, "hit-ratio: {}", self.ratio())?;
        Ok(())
    }
}