use libc::srand;
use std::borrow::{Borrow, BorrowMut};
use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fmt::{Debug, Display, Formatter};
use std::hash::Hash;
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
    items: HashSet<Rc<K>>,
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
pub struct LFUCacheOptions {
    pub min_frequency: usize,
    pub max_capacity: usize,
}

impl Default for LFUCacheOptions {
    fn default() -> Self {
        Self {
            min_frequency: 1,
            max_capacity: usize::MAX,
        }
    }
}

#[derive(Clone)]
struct LFUCache<K, V> {
    options: LFUCacheOptions,
    head: Rc<RefCell<Node<K>>>,
    items: HashMap<Rc<K>, NodeValue<K, V>>,
}

impl<K, V> LFUCache<K, V>
where
    K: Eq + Hash + Clone + Debug,
    V: Debug,
{
    fn new() -> Self {
        Self {
            options: Default::default(),
            head: Default::default(),
            items: Default::default(),
        }
    }

    fn with_options(options: LFUCacheOptions) -> Self {
        Self {
            options,
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

    fn get(&mut self, key: &K) -> Option<&V> {
        let key = if let Some(tmp) = self.items.get_key_value(key) {
            tmp.0.clone()
        } else {
            return None;
        };

        let tmp = if let Some(tmp) = self.items.get_mut(&key) {
            tmp
        } else {
            return None;
        };

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

        return Some(&tmp.data);
    }

    fn insert(&mut self, key: K, value: V) -> Result<(), CacheError> {
        if self.items.contains_key(&key) {
            return Err(CacheError::DuplicateEntry);
        }
        let next_freg = self.head.as_ref().borrow_mut().next.clone();
        let mut freq = match next_freg {
            None => Node::new(1, &mut self.head, None),
            Some(freq) => {
                if freq.as_ref().borrow().freq_count != self.options.min_frequency {
                    Node::new(1, &mut self.head, Some(freq))
                } else {
                    freq
                }
            }
        };
        let key = Rc::new(key);
        freq.as_ref().borrow_mut().items.insert(key.clone());
        let value = NodeValue::new(value, freq);
        self.items.insert(key.clone(), value);
        Ok(())
    }

    fn evict(&mut self) -> Vec<(K, V)> {
        if self.items.len() <= self.options.max_capacity {
            return Vec::new();
        }
        if self.items.is_empty() {
            return Vec::new();
        }
        let mut removed = Vec::with_capacity(self.items.len() - self.options.max_capacity);

        let mut node = self.head.as_ref().borrow_mut().next.clone();
        while let Some(next_node) = node {
            for key in next_node.as_ref().borrow().items.iter() {
                if self.items.len() > self.options.max_capacity {
                    if let Some(r) = self.items.remove(key) {
                        removed.push((key.as_ref().clone(), r.data))
                    };
                } else {
                    break;
                }
            }
            for (k, _) in removed.iter() {
                next_node.as_ref().borrow_mut().items.remove(k);
            }
            if next_node.as_ref().borrow().items.is_empty() {
                Self::delete_node(next_node.clone());
            }
            println!(
                "{} Rc::strong_count {}",
                next_node.as_ref().borrow().freq_count,
                Rc::strong_count(&next_node)
            );
            println!(
                "{} Rc::weak_count {}",
                next_node.as_ref().borrow().freq_count,
                Rc::weak_count(&next_node)
            );
            node = next_node.as_ref().borrow().next.clone()
        }
        removed
    }

    fn flatten(&self) -> BTreeMap<usize, HashSet<Rc<K>>> {
        let mut map = BTreeMap::new();
        let mut node = self.head.as_ref().borrow().next.clone();
        while let Some(n) = node {
            map.insert(
                n.as_ref().borrow().freq_count,
                n.as_ref().borrow().items.clone(),
            );
            node = n.as_ref().borrow().next.clone();
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
    use crate::lfu_cache::{LFUCache, LFUCacheOptions};
    use std::collections::BTreeMap;
    use std::time::Duration;

    #[test]
    fn basic_insertions() {
        let mut cache = LFUCache::new();
        cache.insert(1, 10).unwrap();
        cache.insert(2, 20).unwrap();
        cache.insert(3, 30).unwrap();
        cache.insert(4, 40).unwrap();
        println!("{:#?}", cache);
        cache.get(&2);
        cache.get(&2);
        cache.get(&3);
        cache.get(&3);
        cache.get(&1);
        cache.get(&1);
        cache.get(&1);
        println!("{:?}", cache);
    }

    #[test]
    fn basic_insertions_with_eviction() {
        let mut cache = LFUCache::with_options(LFUCacheOptions {
            min_frequency: 1,
            max_capacity: 2,
        });
        for i in 1..999999 {
            cache.insert(i, i).unwrap();
        }
    }

    #[test]
    fn basic_insertions_with_map() {
        let mut cache = BTreeMap::new();
        for i in 1..999999 {
            cache.insert(i, i);
        }
    }
}
