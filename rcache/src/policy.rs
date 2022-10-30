use std::collections::HashMap;
use std::ops::BitXor;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::mpsc::{Receiver, SyncSender};
use std::thread::JoinHandle;

use parking_lot::Mutex;

use crate::{Metrics, MetricType, ring};
use crate::bloom::Bloom;
use crate::cm_sketch::CMSketch;

const LFU_SAMPLE: usize = 5;

#[derive(Copy, Clone, Debug)]
pub(crate) struct PolicyPair(u64, i64);

impl PolicyPair {
    pub(crate) fn key(&self) -> u64 {
        self.0
    }

    pub(crate) fn cost(&self) -> i64 {
        self.1
    }
}

pub(crate) trait Policy: ring::Consumer {
    fn add(&self, key: u64, cost: i64) -> (Option<Vec<Item>>, bool);
    fn has(&self, key: &u64) -> bool;
    fn remove(&self, key: &u64);
    fn cap(&self) -> usize;
    fn close(&self);
    fn update(&self, key: u64, cost: i64);
    fn cost(&self, key: &u64) -> i64;
    fn clear(&self);
    fn max_cost(&self) -> i64;
    fn update_max_cost(&self, cost: i64);
}

struct Inner {
    admit: TinyLFU,
    evict: SampledLFU,
}

#[derive(Debug, Copy, Clone)]
pub(crate) struct Item {
    pub(crate) key: u64,
    pub(crate) conflict: u64,
    pub(crate) cost: i64,
}

fn process_op(
    close: Arc<AtomicBool>,
    items_recv: Receiver<Vec<u64>>,
    p: Arc<Mutex<Inner>>,
) -> JoinHandle<()> {
    std::thread::spawn(move || loop {
        if close.load(Ordering::Acquire) {
            break;
        }
        if let Ok(items) = items_recv.try_recv() {
            let mut p = p.lock();
            println!("Admit {:?}", items);
            p.admit.push(items);
            println!("Admited")
        }
    })
}

pub(crate) struct DefaultPolicy {
    inner: Arc<Mutex<Inner>>,
    items_channel: SyncSender<Vec<u64>>,
    is_closed: Arc<AtomicBool>,
    processor: JoinHandle<()>,
    max_cost: Arc<AtomicI64>,
    metrics: Option<Metrics>
}

impl DefaultPolicy {
    pub(crate) fn new(num_counters: u64, max_cost: i64) -> Self {
        Self::new_with_metrics(num_counters, max_cost, None)
    }
    pub(crate) fn new_with_metrics(num_counters: u64, max_cost: i64, metrics: Option<Metrics>) -> Self {
        let is_closed = Arc::new(AtomicBool::new(false));
        let max_cost = Arc::new(AtomicI64::new(max_cost));
        let inner = Arc::new(Mutex::new(Inner {
            admit: TinyLFU::new(num_counters),
            evict: SampledLFU::new(max_cost.clone(), metrics.clone()),
        }));
        let (items_channel, receiver) = std::sync::mpsc::sync_channel(3);
        let processor = process_op(is_closed.clone(), receiver, inner.clone());
        Self {
            inner,
            items_channel,
            is_closed,
            processor,
            max_cost,
            metrics
        }
    }
    pub(crate) fn close(&self) {
        self.is_closed.store(true, Ordering::Release);
    }
}

impl ring::Consumer for DefaultPolicy {
    fn push(&self, keys: Vec<u64>) -> bool {
        let closed = self.is_closed.load(Ordering::Acquire);
        if closed {
            return false;
        }

        if keys.is_empty() {
            return true;
        }
        let key = keys[0];
        let keys_len = keys.len() as u64;
        if self.items_channel.send(keys).is_ok() {
            if let Some(metrics) = self.metrics.as_ref() {
                metrics.add(MetricType::KeepGets, key, keys_len);
            }
            return true
        }
        if let Some(metrics) = self.metrics.as_ref() {
            metrics.add(MetricType::DropGets, key, keys_len);
        }
        return false
    }
}

impl Policy for DefaultPolicy {
    /// *add* decides whether the item with the given key and cost should be accepted by
    /// the policy. It returns the list of victims that have been evicted and a boolean
    /// indicating whether the incoming item should be accepted.
    fn add(&self, key: u64, cost: i64) -> (Option<Vec<Item>>, bool) {
        let mut p = self.inner.lock();

        // Cannot add an item bigger than entire cache.
        if cost > p.evict.max_cost() {
            return (None, false);
        }

        // No need to go any further if the item is already in the cache.
        if p.evict.update_if_has(key, cost) {
            return (None, false);
        }

        // If the execution reaches this point, the key doesn't exist in the cache.
        // Calculate the remaining room in the cache (usually bytes).
        let mut room = p.evict.room_left(cost);
        if room >= 0 {
            // There's enough room in the cache to store the new item without
            // overflowing. Do that now and stop here.
            p.evict.add(key, cost);
            if let Some(metrics) = self.metrics.as_ref() {
                metrics.add(MetricType::CostAdd, key, cost as u64);
            }
            return (None, true);
        }

        // incHits is the hit count for the incoming item.
        let inc_hits = p.admit.estimate(key);
        // sample is the eviction candidate pool to be filled via random sampling.
        let mut sample = Vec::with_capacity(LFU_SAMPLE);
        // As items are evicted they will be appended to victims.
        let mut victims = Vec::new();

        // Delete victims until there's enough space or a minKey is found that has
        // more hits than incoming item.
        while room < 0 {
            // Fill up empty slots in sample.
            p.evict.fill_sample(&mut sample);
            // Find minimally used item in sample.
            let (mut min_key, mut min_hits, mut min_id, mut min_cost) = (0, i64::MAX, 0, 0);
            for (i, pair) in sample.iter().enumerate() {
                // Look up hit count for sample key.
                let hits = p.admit.estimate(pair.key());
                if hits < min_hits {
                    (min_key, min_hits, min_id, min_cost) = (pair.0, hits, i, pair.1)
                }
            }

            // If the incoming item isn't worth keeping in the policy, reject.
            if inc_hits < min_hits {
                if let Some(metrics) = self.metrics.as_ref() {
                    metrics.add(MetricType::RejectSets, key, 1);
                }
                return (Some(victims), false);
            }

            // Delete the victim from metadata.
            p.evict.remove(&min_key);

            // Delete the victim from sample.
            sample[min_id] = sample[sample.len() - 1];
            sample.truncate(sample.len() - 1);

            // Store victim in evicted victims slice.
            victims.push(Item {
                key: min_key,
                conflict: 0,
                cost: min_cost,
            });

            room = p.evict.room_left(cost);
        }
        // Add Key and Cost to sample
        p.evict.add(key, cost);
        if let Some(metrics) = self.metrics.as_ref() {
            metrics.add(MetricType::CostAdd, key, cost as u64);
        }
        return (Some(victims), true);
    }

    fn has(&self, key: &u64) -> bool {
        let p = self.inner.lock();

        p.evict.key_costs.contains_key(key)
    }

    fn remove(&self, key: &u64) {
        let mut p = self.inner.lock();

        p.evict.remove(key);
    }

    fn cap(&self) -> usize {
        let p = self.inner.lock();

        (p.evict.max_cost() - p.evict.used) as usize
    }

    fn close(&self) {
        self.is_closed.store(true, Ordering::Release);
    }

    fn update(&self, key: u64, cost: i64) {
        let mut p = self.inner.lock();
        p.evict.update_if_has(key, cost);
    }

    fn cost(&self, key: &u64) -> i64 {
        let p = self.inner.lock();
        if let Some(cost) = p.evict.key_costs.get(key) {
            return *cost;
        }
        -1
    }

    fn clear(&self) {
        let mut p = self.inner.lock();
        p.evict.clear();
        p.admit.clear();
    }

    fn max_cost(&self) -> i64 {
        self.max_cost.load(Ordering::Acquire)
    }

    fn update_max_cost(&self, cost: i64) {
        self.max_cost.store(cost, Ordering::Release)
    }
}

impl Drop for DefaultPolicy {
    fn drop(&mut self) {
        self.close()
    }
}

pub(crate) struct SampledLFU {
    max_cost: Arc<AtomicI64>,
    used: i64,
    key_costs: HashMap<u64, i64>,
    metrics: Option<Metrics>,
}

impl SampledLFU {
    pub(crate) fn new(max_cost: Arc<AtomicI64>, metrics: Option<Metrics>) -> Self {
        Self {
            max_cost,
            used: 0,
            key_costs: Default::default(),
            metrics,
        }
    }
    pub(crate) fn max_cost(&self) -> i64 {
        self.max_cost.load(Ordering::Acquire)
    }

    pub(crate) fn room_left(&self, cost: i64) -> i64 {
        self.max_cost() - (self.used + cost)
    }

    pub(crate) fn fill_sample(&self, input: &mut Vec<PolicyPair>) {
        if input.len() >= LFU_SAMPLE {
            return;
        }
        for (key, cost) in &self.key_costs {
            input.push(PolicyPair(*key, *cost));
            if input.len() >= LFU_SAMPLE {
                return;
            }
        }
    }

    pub(crate) fn remove(&mut self, key: &u64) -> Option<i64> {
        let cost = *self.key_costs.get(key)?;
        self.used -= cost;
        let out = self.key_costs.remove(key);
        if let Some(metrics) = self.metrics.as_ref() {
            metrics.add(MetricType::CostEvict, *key, cost as u64);
            metrics.add(MetricType::KeyEvict, *key, 1);
        }
        out
    }

    pub(crate) fn add(&mut self, key: u64, cost: i64) {
        self.key_costs.insert(key, cost);
        self.used += cost;
    }

    pub(crate) fn update_if_has(&mut self, key: u64, cost: i64) -> bool {
        if let Some(prev) = self.key_costs.get_mut(&key) {
            if let Some(metrics) = self.metrics.as_ref() {
                let diff = *prev - cost;
                metrics.add(MetricType::CostAdd, key, diff as u64)
            }

            self.used += cost - *prev;
            *prev = cost;
            return true;
        }
        false
    }

    pub(crate) fn clear(&mut self) {
        self.used = 0;
        self.key_costs.clear()
    }
}

pub(crate) struct TinyLFU {
    freq: CMSketch,
    door: Bloom,
    incrs: i64,
    reset_at: i64,
}

impl TinyLFU {
    pub(crate) fn new(num_counters: u64) -> Self {
        Self {
            freq: CMSketch::new(num_counters),
            door: Bloom::new(num_counters as usize, 0.01),
            incrs: 0,
            reset_at: num_counters as i64,
        }
    }

    fn push(&mut self, keys: Vec<u64>) {
        for key in keys {
            self.increment(key)
        }
    }
    pub(crate) fn estimate(&mut self, key: u64) -> i64 {
        let mut hits = self.freq.estimate(&key);
        if self.door.check(&key) {
            hits += 1;
        }
        hits
    }

    pub(crate) fn increment(&mut self, key: u64) {
        if self.door.check_and_set(key) {
            println!("Increment {}", key);
            self.freq.increment(key)
        }
        self.incrs += 1;
        if self.incrs >= self.reset_at {
            self.reset()
        }
    }

    pub(crate) fn reset(&mut self) {
        self.incrs = 0;
        self.freq.clear();
        self.door.clear();
    }

    pub(crate) fn clear(&mut self) {
        self.incrs = 0;
        self.freq.clear();
        self.door.clear();
    }
}

#[cfg(test)]
mod test {
    use std::ops::Not;
    use std::time::Duration;

    use crate::DefaultPolicy;
    use crate::policy::Policy;
    use crate::ring::Consumer;

    #[test]
    fn process_items() {
        let policy = DefaultPolicy::new(100, 10);
        assert!(policy.items_channel.send(vec![1, 2, 2]).is_ok());
        std::thread::sleep(Duration::from_millis(10));
        {
            let mut p = policy.inner.lock();
            assert_eq!(2, p.admit.estimate(2));
            assert_eq!(1, p.admit.estimate(1));
        }
        policy.close();
        policy.items_channel.send(vec![3, 3, 3]);
        std::thread::sleep(Duration::from_millis(10));
        {
            let mut p = policy.inner.lock();
            assert_eq!(0, p.admit.estimate(3));
        }
    }

    #[test]
    fn push() {
        let policy = DefaultPolicy::new(100, 10);
        assert!(policy.push(vec![]));
        let mut keep_count = 0;
        for _i in 0..10 {
            if policy.push(vec![1, 2, 3, 4, 5]) {
                keep_count += 1;
            }
        }
        assert_ne!(0, keep_count)
    }

    #[test]
    fn add() {
        let policy = DefaultPolicy::new(1000, 100);
        let (victims, added) = policy.add(1, 101);
        if victims.is_some() || added {
            panic!("can't add an item bigger than entire cache")
        }

        {
            let mut policy = policy.inner.lock();
            policy.evict.add(1, 1);
            policy.admit.increment(1);
            policy.admit.increment(2);
            policy.admit.increment(3);
        }

        let (victims, added) = policy.add(1, 1);
        assert!(victims.is_none());
        assert!(added.not());

        let (victims, added) = policy.add(2, 20);
        assert!(victims.is_none());
        assert!(added);

        let (victims, added) = policy.add(3, 90);
        assert!(victims.is_some());
        assert!(added);

        let (victims, added) = policy.add(4, 20);
        assert!(victims.is_some());
        assert!(added.not());
    }
}
