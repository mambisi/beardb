use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt::Debug;
use std::hash::Hash;
use std::num::NonZeroUsize;
use std::rc::Rc;

#[derive(Debug)]
pub(crate) struct LRUCache<K, V> {
    queue: VecDeque<HashSet<Rc<K>>>,
    lookup: HashMap<Rc<K>, V>,
    capacity: usize,
}

impl<K, V> LRUCache<K, V> {
    fn with_capacity(capacity: NonZeroUsize) -> Self {
        Self {
            queue: VecDeque::from(vec![HashSet::new(); 3]),
            lookup: HashMap::with_capacity(capacity.get()),
            capacity: capacity.get(),
        }
    }
}

impl<K, V> LRUCache<K, V>
where
    K: Eq + Hash + Clone + Debug + Ord,
    V: Debug,
{
    pub(crate) fn insert(&mut self, key: K, value: V) {
        if let Some(old_value) = self.lookup.get_mut(&key) {
            *old_value = value;
            return;
        }

        if let Some(front) = self.queue.front_mut() {
            if front.len() < self.capacity / 3 {
                let key = Rc::new(key);
                self.lookup.insert(key.clone(), value);
                front.insert(key);
                return;
            }
        }
        for i in self.queue.pop_back().unwrap_or_default() {
            self.lookup.remove(&i);
        }
        let mut head = HashSet::new();
        let key = Rc::new(key);
        self.lookup.insert(key.clone(), value);
        head.insert(key);
        self.queue.push_front(head)
    }

    pub(crate) fn get(&mut self, key: &K) -> Option<&V> {
        let (key, value) = self.lookup.get_key_value(key)?;

        let front = self.queue.front_mut()?;

        if !front.contains(key) {
            front.insert(key.clone());
        }

        for set in self.queue.iter_mut().skip(1) {
            set.remove(key);
        }
        Some(value)
    }
}

#[test]
fn basic() {
    let mut cache = LRUCache::with_capacity(NonZeroUsize::new(6).unwrap());
    cache.insert(1, 2);
    println!("{:?}", cache);
    cache.insert(3, 4);
    cache.insert(5, 6);
    cache.insert(7, 8);
    cache.insert(9, 10);
    cache.insert(11, 12);

    println!("{:?}", cache);

    cache.get(&7);
    cache.get(&9);
    cache.get(&11);

    println!("{:?}", cache);

    cache.insert(12, 13);
    println!("{:?}", cache);
    cache.insert(14, 15);
    println!("{:?}", cache);
    cache.get(&11);
    cache.get(&9);
    println!("{:?}", cache);
    cache.insert(15, 16);
    println!("{:?}", cache);
}
