use crate::cmp::{Comparator, DefaultComparator};
use crate::error::Error;
use crate::iter::Iter;
use bumpalo::Bump;
use rand::prelude::StdRng;
use rand::{RngCore, SeedableRng};
use std::cell::RefCell;
use std::cmp::Ordering;
use std::fmt::{Debug, Display, Formatter};
use std::marker::PhantomData;
use std::rc::Rc;

const MAX_HEIGHT: usize = 12;
const BRANCHING_FACTOR: u32 = 4;

#[derive(Debug)]
struct Node {
    key: *const [u8],
    skips: *mut Vec<Option<*mut Node>>,
}

impl Node {
    fn next(&self, n: usize) -> Option<*mut Node> {
        unsafe { (*self.skips)[n] }
    }

    fn set_next(&mut self, n: usize, node: Option<*mut Node>) {
        unsafe { (*self.skips)[n] = node }
    }

    fn key(&self) -> &[u8] {
        unsafe { &(*self.key) }
    }
}

// impl Debug for InnerSkipList {
//
// }

pub struct InnerSkipList {
    cmp: Rc<Box<dyn Comparator>>,
    arena: Bump,
    head: Box<Node>,
    rand: StdRng,
    max_height: usize,
    len: usize,
}

impl Debug for InnerSkipList {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut list = f.debug_map();

        let mut current = self.head.as_ref() as *const Node;
        loop {
            unsafe {
                let next = (*current).next(0);
                if let Some(next) = next {
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
                if let Some(next) = next {
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
    pub fn new(arena: Bump, cmp: Rc<Box<dyn Comparator>>) -> InnerSkipList {
        let head = Box::new(Node {
            key: arena.alloc_slice_copy(&[]),
            skips: arena.alloc(vec![None; MAX_HEIGHT]),
        });
        InnerSkipList {
            cmp,
            arena,
            head,
            rand: StdRng::seed_from_u64(0xdeadbeef),
            max_height: 1,
            len: 0,
        }
    }

    fn new_node(&mut self, height: usize, key: &[u8]) -> *mut Node {
        let key = self.arena.alloc_slice_copy(key);
        self.arena.alloc(Node {
            key,
            skips: self.arena.alloc(vec![None; height]),
        })
    }

    fn random_height(&mut self) -> usize {
        let mut height = 1;
        while height < MAX_HEIGHT && self.rand.next_u32() % BRANCHING_FACTOR == 0 {
            height += 1;
        }
        height
    }

    fn find_greater_or_equal(&self, key: &[u8]) -> crate::Result<Option<&Node>> {
        let mut current = self.head.as_ref() as *const Node;
        let mut level = self.max_height - 1;

        loop {
            unsafe {
                let next = (*current).next(level);
                if let Some(next) = next {
                    match self.cmp.cmp(&(*(*next).key), key)? {
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
            }
            if level == 0 {
                break;
            }
            level -= 1;
        }

        unsafe {
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
        let mut level = self.max_height - 1;
        loop {
            unsafe {
                if let Some(next) = (*current).next(level) {
                    if (*next).key() < key {
                        current = next;
                        continue;
                    }
                }
            }
            if level == 0 {
                break;
            }
            level -= 1;
        }
        unsafe {
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
        let mut level = self.max_height - 1;

        loop {
            unsafe {
                let next = (*current).next(level);
                if let Some(next) = next {
                    current = next;
                    continue;
                }
            }
            if level == 0 {
                break;
            }
            level -= 1;
        }

        unsafe {
            if current == self.head.as_ref() {
                None
            } else {
                current.as_ref()
            }
        }
    }

    fn insert(&mut self, key: &[u8]) -> crate::Result<()> {
        let mut prevs = std::vec![None; MAX_HEIGHT];
        let mut current = self.head.as_mut() as *mut Node;
        let mut level = self.max_height - 1;
        loop {
            unsafe {
                if let Some(next) = (*current).next(level) {
                    let ord = self.cmp.cmp(&(*(*next).key), key)?;

                    if ord == Ordering::Equal {
                        return Err(Error::DuplicateEntry);
                    }
                    if ord == Ordering::Less {
                        current = next;
                        continue;
                    }
                }
            }
            prevs[level] = Some(current);
            if level == 0 {
                break;
            } else {
                level -= 1;
            }
        }

        let height = self.random_height();
        let current_height = self.max_height;
        prevs.resize(height, None);
        if height > current_height {
            for prev in prevs.iter_mut().skip(current_height) {
                *prev = Some(self.head.as_mut())
            }
            self.max_height = height;
        }

        current = self.new_node(height, key);
        for (i, prev) in prevs.iter().flatten().enumerate() {
            unsafe {
                (*current).set_next(i, (**prev).next(i));
                (**prev).set_next(i, Some(current));
            }
        }

        self.len += 1;
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
        self.len
    }
}

pub struct SkipList {
    inner: Rc<RefCell<InnerSkipList>>,
}

impl SkipList {
    pub(crate) fn new(cmp: Rc<Box<dyn Comparator>>) -> Self {
        let arena = Bump::new();
        Self {
            inner: Rc::new(RefCell::new(InnerSkipList::new(arena, cmp))),
        }
    }

    pub(crate) fn new_in_arena(arena: Bump, cmp: Rc<Box<dyn Comparator>>) -> Self {
        Self {
            inner: Rc::new(RefCell::new(InnerSkipList::new(arena, cmp))),
        }
    }

    pub(crate) fn default() -> Self {
        Self::new(Rc::new(Box::new(DefaultComparator)))
    }

    pub(crate) fn insert(&mut self, key: &[u8]) -> crate::Result<()> {
        self.inner.borrow_mut().insert(key)
    }

    pub(crate) fn contains(&self, key: &[u8]) -> crate::Result<bool> {
        self.inner.borrow().contains(key)
    }

    pub(crate) fn len(&self) -> usize {
        self.inner.borrow().len()
    }

    pub(crate) fn allocated_bytes(&self) -> usize {
        self.inner.borrow().arena.allocated_bytes()
    }

    fn iter<'a>(&self) -> Box<dyn 'a + Iter<Item = &'a [u8]>> {
        Box::new(SkipMapIterator {
            list: self.inner.clone(),
            node: self.inner.borrow().head.as_ref() as *const Node,
            data_: Default::default(),
        })
    }
}

struct SkipMapIterator<'a> {
    list: Rc<RefCell<InnerSkipList>>,
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
        self.is_valid().is_ok()
    }

    fn prev(&mut self) -> crate::Result<()> {
        self.is_valid()?;
        unsafe {
            match self.list.borrow().find_less_than(&(*(*self.node).key))? {
                None => self.node = std::ptr::null(),
                Some(node) => self.node = node,
            }
        }
        Ok(())
    }

    fn next(&mut self) -> crate::Result<()> {
        self.is_valid()?;
        unsafe {
            match (*self.node).next(0) {
                None => self.node = std::ptr::null(),
                Some(node) => self.node = node,
            }
        }
        Ok(())
    }

    fn current(&self) -> crate::Result<Option<Self::Item>> {
        self.is_valid()?;
        unsafe { Ok(Some(&(*(*self.node).key))) }
    }

    fn seek(&mut self, target: &[u8]) -> crate::Result<()> {
        match self.list.borrow().find_greater_or_equal(target)? {
            None => self.node = std::ptr::null(),
            Some(node) => self.node = node,
        }
        Ok(())
    }

    fn seek_to_first(&mut self) {
        match self.list.borrow().head.next(0) {
            None => self.node = std::ptr::null(),
            Some(node) => self.node = node,
        }
    }

    fn seek_to_last(&mut self) {
        match self.list.borrow().find_last() {
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
    use std::rc::Rc;

    pub fn make_skipmap() -> InnerSkipList {
        let arena = Bump::new();
        let mut skm = InnerSkipList::new(arena, Rc::new(Box::new(DefaultComparator)));
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
        assert!(iter.next().is_ok());
        assert!(iter.valid());
        assert_eq!(current_key_val(&iter).unwrap(), "aba".as_bytes());
        iter.seek(&"abz".as_bytes());
        assert_eq!(current_key_val(&iter).unwrap(), "abz".as_bytes());
        // go back to beginning
        iter.seek(&"aba".as_bytes());
        assert_eq!(current_key_val(&iter).unwrap(), "aba".as_bytes());

        iter.seek(&"".as_bytes());
        assert!(iter.valid());
        assert!(iter.prev().is_ok());
        assert!(!iter.valid());

        while iter.next().is_ok() {}
        assert!(!iter.valid());
        assert!(iter.prev().is_err());
        assert_eq!(current_key_val(&iter), None);
    }

    fn current_key_val<'a>(iter: &'a Box<dyn 'a + Iter<Item = &[u8]>>) -> Option<&'a [u8]> {
        return match iter.current() {
            Ok(Some(t)) => Some(t),
            _ => None,
        };
    }
}
