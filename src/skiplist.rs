use crate::cmp::{Comparator, DefaultComparator};
use crate::error::Error;
use crate::iter::Iter;
use bumpalo::Bump;
use rand::prelude::StdRng;
use rand::{RngCore, SeedableRng};
use std::cmp::Ordering;
use std::fmt::{Debug, Display, Formatter};
use std::marker::PhantomData;
use std::sync::atomic::{AtomicPtr, AtomicUsize, Ordering as MemoryOrdering};
use std::sync::Arc;
const MAX_HEIGHT: usize = 12;
const BRANCHING_FACTOR: u32 = 4;

#[derive(Debug)]
struct Node {
    key: *const [u8],
    skips: *const Vec<AtomicPtr<Node>>,
}

impl Node {
    fn next(&self, n: usize) -> *const Node {
        unsafe { (*self.skips)[n].load(MemoryOrdering::Acquire) }
    }

    fn set_next(&self, n: usize, node: *const Node) {
        unsafe { (*self.skips)[n].store(node as *mut Node, MemoryOrdering::Release) }
    }

    fn nb_next(&self, n: usize) -> *const Node {
        unsafe { (*self.skips)[n].load(MemoryOrdering::Relaxed) }
    }

    fn nb_set_next(&self, n: usize, node: *const Node) {
        unsafe { (*self.skips)[n].store(node as *mut Node, MemoryOrdering::Relaxed) }
    }

    fn key(&self) -> &[u8] {
        unsafe { &(*self.key) }
    }
}

pub struct InnerSkipList {
    cmp: Arc<Box<dyn Comparator>>,
    arena: Bump,
    head: Box<Node>,
    max_height: AtomicUsize,
    len: AtomicUsize,
}

impl Debug for InnerSkipList {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut list = f.debug_map();

        let mut current = self.head.as_ref() as *const Node;
        loop {
            unsafe {
                let next = (*current).next(0);
                if !next.is_null() {
                    list.entry(&(next), &(*next));
                    current = next;
                } else {
                    break;
                }
            }
        }
        list.finish()
    }
}

impl Display for InnerSkipList {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        use std::fmt::Write;
        let mut w = String::new();
        unsafe {
            let mut current = self.head.as_ref() as *const Node;
            loop {
                let next = (*current).next(0);
                if !next.is_null() {
                    current = next;
                    writeln!(
                        &mut w,
                        "{:?} {:?} - {:?}",
                        current,
                        (*current).key,
                        (*(*current).skips)
                    )?;
                } else {
                    break;
                }
            }
        }
        f.write_str(&w)
    }
}

impl InnerSkipList {
    pub fn new(arena: Bump, cmp: Arc<Box<dyn Comparator>>) -> InnerSkipList {
        let skips = arena.alloc(Vec::with_capacity(MAX_HEIGHT));
        for _ in 0..MAX_HEIGHT {
            skips.push(AtomicPtr::default())
        }
        let head = Box::new(Node {
            key: arena.alloc_slice_copy(&[]),
            skips,
        });
        InnerSkipList {
            cmp,
            arena,
            head,
            max_height: AtomicUsize::new(1),
            len: AtomicUsize::default(),
        }
    }

    fn new_node(&self, height: usize, key: &[u8]) -> *mut Node {
        let key = self.arena.alloc_slice_copy(key);
        let skips = self.arena.alloc(Vec::with_capacity(height));
        for _ in 0..height {
            skips.push(AtomicPtr::default())
        }
        self.arena.alloc(Node { key, skips })
    }

    fn random_height(&self) -> usize {
        let mut height = 1;
        let mut rand = StdRng::seed_from_u64(0xdeadbeef);
        while height < MAX_HEIGHT && rand.next_u32() % BRANCHING_FACTOR == 0 {
            height += 1;
        }
        height
    }

    fn max_height(&self) -> usize {
        self.max_height.load(MemoryOrdering::Relaxed)
    }

    fn find_greater_or_equal(&self, key: &[u8]) -> crate::Result<Option<&Node>> {
        let mut current = self.head.as_ref() as *const Node;
        let mut level = self.max_height() - 1;
        unsafe {
            loop {
                let next = (*current).next(level);
                if !next.is_null() {
                    match self.cmp.cmp((*next).key(), key)? {
                        Ordering::Less => {
                            current = next;
                            continue;
                        }
                        Ordering::Equal => return Ok(next.as_ref()),
                        Ordering::Greater => {
                            if level == 0 {
                                return Ok(next.as_ref());
                            }
                        }
                    }
                }

                if level == 0 {
                    break;
                }
                level -= 1;
            }

            Ok(if current == self.head.as_ref() {
                None
            } else if (*(*current).key).lt(key) {
                None
            } else {
                current.as_ref()
            })
        }
    }

    fn find_less_than(&self, key: &[u8]) -> crate::Result<Option<&Node>> {
        let mut current = self.head.as_ref() as *const Node;
        let mut level = self.max_height() - 1;
        unsafe {
            loop {
                let next = (*current).next(level);
                if !next.is_null() {
                    if (*next).key() < key {
                        current = next;
                        continue;
                    }
                }

                if level == 0 {
                    break;
                }
                level -= 1;
            }
            Ok(if current == self.head.as_ref() {
                None
            } else if self.cmp.cmp((*current).key(), key)? != Ordering::Less {
                None
            } else {
                current.as_ref()
            })
        }
    }

    fn find_last(&self) -> Option<&Node> {
        let mut current = self.head.as_ref() as *const Node;
        let mut level = self.max_height() - 1;
        unsafe {
            loop {
                let next = (*current).next(level);
                if !next.is_null() {
                    current = next;
                    continue;
                }

                if level == 0 {
                    break;
                }
                level -= 1;
            }

            if current == self.head.as_ref() {
                None
            } else {
                current.as_ref()
            }
        }
    }

    fn insert(&self, key: &[u8]) -> crate::Result<()> {
        let mut prevs = std::vec![std::ptr::null(); MAX_HEIGHT];
        let mut current = self.head.as_ref() as *const Node;
        let mut level = self.max_height() - 1;
        unsafe {
            loop {
                let next = (*current).next(level);
                if !next.is_null() {
                    let ord = self.cmp.cmp((*next).key(), key)?;

                    if ord == Ordering::Equal {
                        return Err(Error::DuplicateEntry);
                    }
                    if ord == Ordering::Less {
                        current = next;
                        continue;
                    }
                }

                prevs[level] = current;
                if level == 0 {
                    break;
                } else {
                    level -= 1;
                }
            }
        }

        let height = self.random_height();
        let current_height = self.max_height();
        prevs.resize(height, std::ptr::null_mut());
        if height > current_height {
            for prev in prevs.iter_mut().skip(current_height) {
                *prev = self.head.as_ref()
            }
            self.max_height.store(height, MemoryOrdering::Relaxed);
        }

        current = self.new_node(height, key);
        for (i, prev) in prevs.into_iter().enumerate() {
            unsafe {
                (*current).nb_set_next(i, (*prev).nb_next(i));
                (*prev).set_next(i, current);
            }
        }

        self.len.fetch_add(1, MemoryOrdering::Relaxed);
        Ok(())
    }

    fn contains(&self, key: &[u8]) -> crate::Result<bool> {
        if let Some(node) = self.find_greater_or_equal(key)? {
            unsafe {
                return self
                    .cmp
                    .cmp(&(*(*node).key), key)
                    .map(|ord| ord == Ordering::Equal);
            }
        }
        Ok(false)
    }

    fn len(&self) -> usize {
        self.len.load(MemoryOrdering::Relaxed)
    }
}

pub struct SkipList {
    inner: Arc<InnerSkipList>,
}

impl SkipList {
    pub(crate) fn new(cmp: Arc<Box<dyn Comparator>>) -> Self {
        let arena = Bump::new();
        Self {
            inner: Arc::new(InnerSkipList::new(arena, cmp)),
        }
    }

    pub(crate) fn new_in_arena(arena: Bump, cmp: Arc<Box<dyn Comparator>>) -> Self {
        Self {
            inner: Arc::new(InnerSkipList::new(arena, cmp)),
        }
    }

    pub(crate) fn default() -> Self {
        Self::new(Arc::new(Box::new(DefaultComparator)))
    }

    pub(crate) fn insert(&self, key: &[u8]) -> crate::Result<()> {
        self.inner.insert(key)
    }

    pub(crate) fn contains(&self, key: &[u8]) -> crate::Result<bool> {
        self.inner.contains(key)
    }

    pub(crate) fn len(&self) -> usize {
        self.inner.len()
    }

    pub(crate) fn allocated_bytes(&self) -> usize {
        self.inner.arena.allocated_bytes()
    }

    pub(crate) fn iter<'a>(&self) -> Box<dyn 'a + Iter<Item = &'a [u8]>> {
        Box::new(SkipMapIterator {
            list: self.inner.clone(),
            node: self.inner.head.as_ref() as *const Node,
            data_: Default::default(),
        })
    }
}

struct SkipMapIterator<'a> {
    list: Arc<InnerSkipList>,
    node: *const Node,
    data_: PhantomData<&'a ()>,
}

impl<'a> SkipMapIterator<'a> {
    fn is_valid(&self) -> crate::Result<()> {
        if self.node.is_null() {
            return Err(Error::InvalidIterator);
        }
        Ok(())
    }
}

impl<'a> Iter for SkipMapIterator<'a> {
    type Item = &'a [u8];

    fn valid(&self) -> bool {
        !self.node.is_null()
    }

    fn prev(&mut self) {
        if !self.valid() {
            return;
        }
        unsafe {
            match self.list.find_less_than(&(*(*self.node).key)) {
                Ok(Some(node)) => self.node = node,
                _ => self.node = std::ptr::null(),
            }
        }
    }

    fn next(&mut self) {
        if !self.valid() {
            return;
        }
        unsafe { self.node = (*self.node).next(0) }
    }

    fn current(&self) -> Option<Self::Item> {
        if !self.valid() {
            return None;
        }
        unsafe { Some(&(*(*self.node).key)) }
    }

    fn seek(&mut self, target: &[u8]) {
        match self.list.find_greater_or_equal(target) {
            Ok(Some(node)) => self.node = node,
            _ => self.node = std::ptr::null(),
        }
    }

    fn seek_to_first(&mut self) {
        self.node = self.list.head.next(0)
    }

    fn seek_to_last(&mut self) {
        match self.list.find_last() {
            None => self.node = std::ptr::null(),
            Some(node) => self.node = node,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::cmp::DefaultComparator;
    use crate::iter::Iter;
    use crate::skiplist::{InnerSkipList, SkipList};
    use bumpalo::Bump;
    use std::sync::Arc;

    pub fn make_skipmap() -> InnerSkipList {
        let arena = Bump::new();
        let mut skm = InnerSkipList::new(arena, Arc::new(Box::new(DefaultComparator)));
        let keys = vec![
            "aba", "abb", "abc", "abd", "abe", "abf", "abg", "abh", "abi", "abj", "abk", "abl",
            "abm", "abn", "abo", "abp", "abq", "abr", "abs", "abt", "abu", "abv", "abw", "abx",
            "aby", "abz",
        ];

        for k in keys {
            skm.insert(k.as_bytes()).unwrap();
        }
        skm
    }

    pub fn make_skipmap_t() -> SkipList {
        let mut skm = SkipList::default();
        let keys = vec![
            "aba", "abb", "abc", "abd", "abe", "abf", "abg", "abh", "abi", "abj", "abk", "abl",
            "abm", "abn", "abo", "abp", "abq", "abr", "abs", "abt", "abu", "abv", "abw", "abx",
            "aby", "abz",
        ];

        for k in keys {
            skm.insert(k.as_bytes()).unwrap();
        }
        skm
    }

    #[test]
    fn test_insert() {
        let skm = make_skipmap();
        assert_eq!(skm.len(), 26);
        println!("{}", skm)
    }

    #[test]
    fn test_no_dupes() {
        let mut skm = make_skipmap();
        assert!(skm.insert("abc".as_bytes()).is_err());
        assert!(skm.insert("abf".as_bytes()).is_err());
    }

    #[test]
    fn test_find() {
        let skm = make_skipmap();
        assert_eq!(
            skm.find_greater_or_equal("abf".as_bytes())
                .unwrap()
                .unwrap()
                .key(),
            "abf".as_bytes()
        );
        assert!(skm
            .find_greater_or_equal(&"ab{".as_bytes().to_vec())
            .unwrap()
            .is_none());
        assert_eq!(
            skm.find_greater_or_equal(&"aaa".as_bytes().to_vec())
                .unwrap()
                .unwrap()
                .key(),
            "aba".as_bytes()
        );
        assert_eq!(
            skm.find_greater_or_equal(&"ab".as_bytes())
                .unwrap()
                .unwrap()
                .key(),
            "aba".as_bytes()
        );
        assert_eq!(
            skm.find_greater_or_equal(&"abc".as_bytes())
                .unwrap()
                .unwrap()
                .key(),
            "abc".as_bytes()
        );
        assert!(skm.find_less_than(&"ab0".as_bytes()).unwrap().is_none());
        assert_eq!(
            skm.find_less_than(&"abd".as_bytes())
                .unwrap()
                .unwrap()
                .key(),
            "abc".as_bytes()
        );
        assert_eq!(
            skm.find_less_than(&"ab{".as_bytes())
                .unwrap()
                .unwrap()
                .key(),
            "abz".as_bytes()
        );
    }

    #[test]
    fn test_contains() {
        let skm = make_skipmap();
        println!("Allocated {}", skm.arena.allocated_bytes());
        assert!(skm.contains("aby".as_bytes()).unwrap());
        assert!(skm.contains("abc".as_bytes()).unwrap());
        assert!(skm.contains("abz".as_bytes()).unwrap());
        assert!(!skm.contains("ab{".as_bytes()).unwrap());
        assert!(!skm.contains("123".as_bytes()).unwrap());
        assert!(!skm.contains("aaa".as_bytes()).unwrap());
        assert!(!skm.contains("456".as_bytes()).unwrap());
    }

    #[test]
    fn test_skipmap_iterator_seek_valid() {
        let skm = make_skipmap_t();
        let mut iter = skm.iter();
        iter.next();
        assert!(iter.valid());
        assert_eq!(current_key_val(&iter).unwrap(), "aba".as_bytes());
        iter.seek(&"abz".as_bytes());
        assert_eq!(current_key_val(&iter).unwrap(), "abz".as_bytes());
        // go back to beginning
        iter.seek(&"aba".as_bytes());
        assert_eq!(current_key_val(&iter).unwrap(), "aba".as_bytes());

        iter.seek(&"".as_bytes());
        assert!(iter.valid());
        iter.prev();
        assert!(!iter.valid());

        while iter.valid() {
            iter.next()
        }
        assert!(!iter.valid());
        iter.prev();
        assert_eq!(current_key_val(&iter), None);
    }

    fn current_key_val<'a>(iter: &'a Box<dyn 'a + Iter<Item = &[u8]>>) -> Option<&'a [u8]> {
        iter.current()
    }
}
