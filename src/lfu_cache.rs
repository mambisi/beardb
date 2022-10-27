use indexmap::IndexSet;
use std::borrow::{Borrow, BorrowMut};
use std::cell::{Cell, RefCell};
use std::collections::hash_map::Iter as HashMapIter;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fmt::{Debug, Display, Formatter};
use std::hash::Hash;
use std::num::NonZeroUsize;
use std::rc::Rc;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum CacheError {
    #[error("Key already exists")]
    DuplicateEntry,

    #[error("KeyNotFound")]
    KeyNotFound,
}

/// # LFU Cache implementation
/// based on http://dhruvbird.com/lfu.pdf

#[derive(Debug, Clone)]
struct Node<K> {
    freq_count: usize,
    items: IndexSet<Rc<K>>,
    prev: Option<Rc<RefCell<Node<K>>>>,
    next: Option<Rc<RefCell<Node<K>>>>,
}

impl<K> Default for Node<K> {
    fn default() -> Self {
        Self {
            freq_count: 0,
            items: Default::default(),
            prev: Default::default(),
            next: None,
        }
    }
}

impl<K> Node<K> {
    fn new(
        freq_count: usize,
        prev: &mut Rc<RefCell<Node<K>>>,
        next: Option<Rc<RefCell<Node<K>>>>,
    ) -> Rc<RefCell<Node<K>>> {
        let node = Rc::new(RefCell::new(Node {
            freq_count,
            items: Default::default(),
            prev: Some(prev.clone()),
            next: next.clone(),
        }));
        prev.as_ref().borrow_mut().next = Some(node.clone());

        if let Some(next) = next {
            next.as_ref().borrow_mut().prev = Some(node.clone())
        }
        node
    }
}

impl<K> Drop for Node<K> {
    fn drop(&mut self) {
        if let Some(prev) = self.prev.as_ref() {
            prev.as_ref().borrow_mut().next = self.next.clone()
        }

        if let Some(next) = self.next.as_ref() {
            next.as_ref().borrow_mut().prev = self.prev.clone()
        }
    }
}

#[derive(Debug, Clone)]
struct NodeValue<K, V> {
    data: V,
    parent: Rc<RefCell<Node<K>>>,
}

impl<K, V> NodeValue<K, V> {
    fn new(data: V, parent: Rc<RefCell<Node<K>>>) -> NodeValue<K, V> {
        Self { data, parent }
    }
}

#[derive(Clone)]
pub(crate) struct LFUCache<K, V> {
    capacity: usize,
    head: Rc<RefCell<Node<K>>>,
    items: HashMap<Rc<K>, NodeValue<K, V>>,
}

impl<K, V> LFUCache<K, V>
where
    K: Eq + Hash + Clone + Debug + Ord,
    V: Debug,
{
    pub(crate) fn new() -> Self {
        Self {
            capacity: usize::MAX,
            head: Default::default(),
            items: Default::default(),
        }
    }

    pub(crate) fn with_capacity(capacity: NonZeroUsize) -> Self {
        Self {
            capacity: capacity.get(),
            head: Default::default(),
            items: Default::default(),
        }
    }
    fn delete_node(node: Rc<RefCell<Node<K>>>) {
        if let Some(prev) = node.as_ref().borrow().prev.as_ref() {
            prev.as_ref().borrow_mut().next = node.as_ref().borrow().next.clone()
        }

        if let Some(next) = node.as_ref().borrow().next.as_ref() {
            next.as_ref().borrow_mut().prev = node.as_ref().borrow().prev.clone()
        }
    }

    fn get_node_for_key(&mut self, key: &K) -> Option<&mut NodeValue<K, V>> {
        let key = if let Some(tmp) = self.items.get_key_value(key) {
            tmp.0.clone()
        } else {
            return None;
        };

        let tmp = self.items.get_mut(&key)?;

        let mut freq = tmp.parent.clone();
        let next_freq = tmp.parent.as_ref().borrow_mut().next.clone();

        let next_freg = match next_freq {
            None => {
                let freq_count = freq.as_ref().borrow().freq_count + 1;
                Node::new(freq_count, &mut freq, None)
            }
            Some(next_freg) => {
                if next_freg.as_ptr().eq(&self.head.as_ptr())
                    || next_freg.as_ref().borrow().freq_count
                        != freq.as_ref().borrow().freq_count + 1
                {
                    let freq_count = freq.as_ref().borrow().freq_count + 1;
                    Node::new(freq_count, &mut freq, Some(next_freg))
                } else {
                    next_freg
                }
            }
        };

        freq.as_ref().borrow_mut().items.remove(&key);
        next_freg.as_ref().borrow_mut().items.insert(key);
        tmp.parent = next_freg;
        if freq.as_ref().borrow().items.is_empty() {
            Self::delete_node(freq);
        }
        Some(tmp)
    }

    pub(crate) fn get(&mut self, key: &K) -> Option<&V> {
        let node = self.get_node_for_key(key)?;
        return Some(&node.data);
    }

    fn pop_lfu(&mut self) -> Option<V> {
        if self.items.is_empty() {
            return None;
        }
        let next_freg = self.head.as_ref().borrow_mut().next.clone()?;
        let popped = next_freg.as_ref().borrow_mut().items.pop()?;
        if next_freg.as_ref().borrow().items.is_empty() {
            Self::delete_node(next_freg);
        }
        self.items.remove(&popped).map(|popped| popped.data)
    }

    pub(crate) fn get_mut(&mut self, key: &K) -> Option<&mut V> {
        let node = self.get_node_for_key(key)?;
        return Some(&mut node.data);
    }

    pub(crate) fn remove(&mut self, key: &K) -> Option<V> {
        let tmp = if let Some(tmp) = self.items.remove(key) {
            tmp
        } else {
            return None;
        };

        let mut freq = tmp.parent.clone();
        freq.as_ref().borrow_mut().items.remove(key);
        if freq.as_ref().borrow().items.is_empty() {
            Self::delete_node(freq);
        }
        return Some(tmp.data);
    }

    pub(crate) fn insert(&mut self, key: K, value: V) -> Option<V> {
        let mut evicted = self.remove(&key);
        if self.items.len() + 1 > self.capacity {
            evicted = self.pop_lfu();
        }

        let next_freg = self.head.as_ref().borrow_mut().next.clone();
        let mut freq = match next_freg {
            None => Node::new(0, &mut self.head, None),
            Some(freq) => {
                if freq.as_ref().borrow().freq_count != 0 {
                    Node::new(0, &mut self.head, Some(freq))
                } else {
                    freq
                }
            }
        };
        let key = Rc::new(key);
        freq.as_ref().borrow_mut().items.insert(key.clone());
        let value = NodeValue::new(value, freq);
        self.items.insert(key.clone(), value);
        evicted
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    pub(crate) fn len(&self) -> usize {
        self.items.len()
    }

    pub(crate) fn frequencies(&self) -> Vec<usize> {
        let mut frequencies = Vec::new();
        let mut node = self.head.as_ref().borrow().next.clone();
        while let Some(n) = node {
            frequencies.push(n.as_ref().borrow().freq_count);
            node = n.as_ref().borrow().next.clone();
        }
        frequencies
    }

    pub(crate) fn freq_len(&self) -> usize {
        let mut count = 0;
        let mut node = self.head.as_ref().borrow().next.clone();
        while let Some(n) = node {
            count += 1;
            node = n.as_ref().borrow().next.clone();
        }
        count
    }

    pub(crate) fn clear(&mut self) {
        self.items.clear();
        self.head = Default::default()
    }

    pub(crate) fn iter<'a>(&'a self) -> Box<dyn 'a + Iterator<Item = (&K, &V)>> {
        Box::new(
            self.items
                .iter()
                .map(|(key, node)| (key.as_ref(), &node.data)),
        )
    }

    pub(crate) fn freq_table(&self) -> BTreeMap<usize, &IndexSet<Rc<K>>> {
        let mut map : BTreeMap<usize, &IndexSet<Rc<K>>> = BTreeMap::new();
        // SAFETY: has exclusive reference to self
        unsafe {
            let mut node = &(*self.head.as_ref().as_ptr()).next;
            while let Some(n) = node {
                map.insert(
                    (*n.as_ref().as_ptr()).freq_count,
                    &(*n.as_ref().as_ptr()).items,
                );
                node = &(*n.as_ref().as_ptr()).next;
            }
        }
        map
    }
}

impl<K: Debug, V: Debug> Debug for LFUCache<K, V> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut map = f.debug_map();
        let mut node = self.head.as_ref().borrow_mut().next.clone();
        while let Some(n) = node {
            map.entry(&n.as_ref().borrow().freq_count, &n.as_ref().borrow().items);
            node = n.as_ref().borrow_mut().next.clone();
        }
        map.finish()
    }
}

#[cfg(test)]
mod test {
    #[cfg(test)]
    mod get {
        use crate::lfu_cache::LFUCache;
        use std::borrow::Borrow;

        #[test]
        fn empty() {
            let mut cache = LFUCache::<u64, u64>::new();
            for i in 0..100 {
                assert!(cache.get(&i).is_none())
            }
        }

        #[test]
        fn get_mut() {
            let mut cache = LFUCache::new();
            cache.insert(1, 2);
            assert_eq!(cache.frequencies(), vec![0]);
            *cache.get_mut(&1).unwrap() = 3;
            assert_eq!(cache.frequencies(), vec![1]);
            assert_eq!(cache.get(&1), Some(&3));
        }

        #[test]
        fn getting_is_ok_after_adding_other_value() {
            let mut cache = LFUCache::new();
            cache.insert(1, 2);
            assert_eq!(cache.get(&1), Some(&2));
            cache.insert(3, 4);
            assert_eq!(cache.get(&1), Some(&2));
        }

        #[test]
        fn bounded_alternating_values() {
            let mut cache = LFUCache::with_capacity(8.try_into().unwrap());
            cache.insert(1, 1);
            cache.insert(2, 2);
            for _ in 0..100 {
                cache.get(&1);
                cache.get(&2);
            }

            assert_eq!(cache.len(), 2);
            assert_eq!(cache.frequencies(), vec![100]);
        }
    }

    #[cfg(test)]
    mod insert {
        use crate::lfu_cache::LFUCache;

        #[test]
        fn insert_new() {
            let mut cache = LFUCache::new();

            for i in 0..100 {
                cache.insert(i, i + 100);
            }

            for i in 0..100 {
                assert_eq!(cache.get(&i), Some(&(i + 100)));
                assert!(cache.get(&(i + 100)).is_none());
            }
        }

        #[test]
        fn reinsertion_of_same_key_resets_freq() {
            let mut cache = LFUCache::new();
            cache.insert(1, 1);
            cache.get(&1);
            cache.insert(1, 1);
            assert_eq!(cache.frequencies(), vec![0]);
        }

        #[test]
        fn insert_bounded() {
            let mut cache = LFUCache::with_capacity(20.try_into().unwrap());

            for i in 0..100 {
                cache.insert(i, i + 100);
            }
        }

        #[test]
        fn insert_returns_evicted() {
            let mut cache = LFUCache::with_capacity(20.try_into().unwrap());
            assert_eq!(cache.insert(1, 2), None);
            for _ in 0..10 {
                assert_eq!(cache.insert(3, 4), Some(2));
                assert_eq!(cache.insert(1, 2), Some(4));
            }
        }
    }

    #[cfg(test)]
    mod pop {
        use crate::lfu_cache::LFUCache;

        #[test]
        fn pop() {
            let mut cache = LFUCache::new();
            for i in 0..100 {
                cache.insert(i, i + 100);
            }

            for i in 0..100 {
                assert_eq!(cache.items.len(), 100 - i);
                assert_eq!(cache.pop_lfu(), Some(200 - i - 1));
            }
        }

        #[test]
        fn pop_empty() {
            let mut cache = LFUCache::<i32, i32>::new();
            assert_eq!(None, cache.pop_lfu());
            assert_eq!(None, cache.pop_lfu());
        }
    }

    #[cfg(test)]
    mod remove {
        use crate::lfu_cache::LFUCache;

        #[test]
        fn remove_to_empty() {
            let mut cache = LFUCache::new();
            cache.insert(1, 2);
            assert_eq!(cache.remove(&1), Some(2));
            assert!(cache.is_empty());
            assert_eq!(cache.freq_len(), 0);
        }

        #[test]
        fn remove_empty() {
            let mut cache = LFUCache::<usize, usize>::new();
            assert!(cache.remove(&1).is_none());
        }

        #[test]
        fn remove_to_nonempty() {
            let mut cache = LFUCache::new();
            cache.insert(1, 2);
            cache.insert(3, 4);

            assert_eq!(cache.remove(&1), Some(2));

            assert!(!cache.is_empty());

            assert_eq!(cache.remove(&3), Some(4));

            assert!(cache.is_empty());
            assert_eq!(cache.freq_len(), 0);
        }

        #[test]
        fn remove_middle() {
            let mut cache = LFUCache::new();
            cache.insert(1, 2);
            cache.insert(3, 4);
            cache.insert(5, 6);
            cache.insert(7, 8);
            cache.insert(9, 10);
            cache.insert(11, 12);

            cache.get(&7);
            cache.get(&9);
            cache.get(&11);

            assert_eq!(cache.frequencies(), vec![0, 1]);
            assert_eq!(cache.len(), 6);

            cache.remove(&9);
            assert!(cache.get(&7).is_some());
            assert!(cache.get(&11).is_some());

            cache.remove(&3);
            assert!(cache.get(&1).is_some());
            assert!(cache.get(&5).is_some());
        }

        #[test]
        fn remove_end() {
            let mut cache = LFUCache::new();
            cache.insert(1, 2);
            cache.insert(3, 4);
            cache.insert(5, 6);
            cache.insert(7, 8);
            cache.insert(9, 10);
            cache.insert(11, 12);

            cache.get(&7);
            cache.get(&9);
            cache.get(&11);

            assert_eq!(cache.frequencies(), vec![0, 1]);
            assert_eq!(cache.len(), 6);

            cache.remove(&7);
            assert!(cache.get(&9).is_some());
            assert!(cache.get(&11).is_some());

            cache.remove(&1);
            assert!(cache.get(&3).is_some());
            assert!(cache.get(&5).is_some());
        }

        #[test]
        fn remove_start() {
            let mut cache = LFUCache::new();
            cache.insert(1, 2);
            cache.insert(3, 4);
            cache.insert(5, 6);
            cache.insert(7, 8);
            cache.insert(9, 10);
            cache.insert(11, 12);

            cache.get(&7);
            cache.get(&9);
            cache.get(&11);

            assert_eq!(cache.frequencies(), vec![0, 1]);
            assert_eq!(cache.len(), 6);

            cache.remove(&11);
            assert!(cache.get(&9).is_some());
            assert!(cache.get(&7).is_some());

            cache.remove(&5);
            assert!(cache.get(&3).is_some());
            assert!(cache.get(&1).is_some());
        }

        #[test]
        fn remove_connects_next_owner() {
            let mut cache = LFUCache::new();
            cache.insert(1, 1);
            cache.insert(2, 2);
            assert_eq!(cache.get(&1), Some(&1));
            assert_eq!(cache.remove(&2), Some(2));
            assert_eq!(cache.get(&1), Some(&1));
        }
    }

    #[cfg(test)]
    mod bookkeeping {
        use crate::lfu_cache::LFUCache;
        use std::num::NonZeroUsize;

        #[test]
        fn getting_one_element_has_constant_freq_list_size() {
            let mut cache = LFUCache::new();
            cache.insert(1, 2);
            assert_eq!(cache.freq_len(), 1);

            for _ in 0..100 {
                cache.get(&1);
                assert_eq!(cache.freq_len(), 1);
            }
        }

        #[test]
        fn freq_list_node_merges() {
            let mut cache = LFUCache::new();
            cache.insert(1, 2);
            cache.insert(3, 4);
            assert_eq!(cache.freq_len(), 1);
            assert!(cache.get(&1).is_some());
            assert_eq!(cache.freq_len(), 2);
            assert!(cache.get(&3).is_some());
            assert_eq!(cache.freq_len(), 1);
        }

        #[test]
        fn freq_list_multi_items() {
            let mut cache = LFUCache::new();
            cache.insert(1, 2);
            cache.get(&1);
            cache.get(&1);
            cache.insert(3, 4);
            assert_eq!(cache.freq_len(), 2);
            cache.get(&3);
            assert_eq!(cache.freq_len(), 2);
            cache.get(&3);
            assert_eq!(cache.freq_len(), 1);
        }

        #[test]
        fn clear_is_ok() {
            let mut cache = LFUCache::new();
            for i in 0..10 {
                cache.insert(i, i);
            }

            assert!(!cache.is_empty());

            cache.clear();

            assert!(cache.is_empty());

            for i in 0..10 {
                assert!(cache.get(&i).is_none());
            }
        }

        #[test]
        fn frequecy_halving() {
            let mut cache = LFUCache::new();
            cache.insert(1, 2);
            cache.insert(3, 4);
            cache.insert(5, 6);
            cache.insert(7, 8);
            cache.insert(9, 10);
            cache.insert(11, 12);

            cache.get(&1);
            cache.get(&1);
            cache.get(&5);
            cache.get(&5);
            cache.get(&11);
            cache.get(&11);

            cache.get(&7);
            cache.get(&7);
            cache.get(&7);
            cache.get(&7);
            cache.get(&9);
            cache.get(&9);
            cache.get(&9);
            cache.get(&9);


            //
            // println!("{:?}", cache.frequencies());
            // cache.halve_freq_counts();
            // println!("{:?}", cache.freq_table());
            // cache.halve_freq_counts();
            // println!("{:?}", cache.freq_table());

            // assert_eq!(cache.frequencies(), vec![0, 1]);
            // assert_eq!(cache.len(), 6);
            //
            // cache.remove(&7);
            // assert!(cache.get(&9).is_some());
            // assert!(cache.get(&11).is_some());
            //
            // cache.remove(&1);
            // assert!(cache.get(&3).is_some());
            // assert!(cache.get(&5).is_some());
        }
    }
}
