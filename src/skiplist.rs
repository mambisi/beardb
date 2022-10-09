use crate::iter::Iter;
use bumpalo::{boxed::Box, collections::Vec, vec, Bump};
use rand::prelude::StdRng;
use rand::{RngCore, SeedableRng};
use std::array::IntoIter;
use std::cmp::Ordering;
use std::fmt::{Debug, Display, Formatter, Pointer};
use std::marker::PhantomData;
use std::thread::current;

const MAX_HEIGHT: usize = 12;
const BRANCHING_FACTOR: u32 = 4;

#[derive(Debug)]
struct Node<'a> {
    key: &'a [u8],
    value: &'a [u8],
    skips: Vec<'a, Option<*mut Node<'a>>>,
}

impl<'a> Node<'a> {
    fn next(&self, n: usize) -> Option<*mut Node<'a>> {
        self.skips[n]
    }

    fn set_next(&mut self, n: usize, node: Option<*mut Node<'a>>) {
        self.skips[n] = node
    }
}

pub struct SkipMap<'a> {
    arena: &'a Bump,
    head: Box<'a, Node<'a>>,
    rand: StdRng,
    max_height: usize,
    len: usize,
}

impl<'a> Debug for SkipMap<'a> {
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


impl<'a> Display for SkipMap<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        use std::fmt::Write;
        let mut w = String::new();
        unsafe {
            let mut current = self.head.as_ref() as *const Node;
            loop {

                let next = (*current).next(0);
                if let Some(next) = next {
                    current = next;
                    writeln!(&mut w,
                             "{:?} {:?}/{:?} - {:?}",
                             current,
                             (*current).key,
                             (*current).value,
                             (*current).skips
                    )?;
                } else {
                    break;
                }
            }
        }
        f.write_str(&w)
    }
}

impl<'a> SkipMap<'a> {
    pub fn new(arena: &'a Bump) -> SkipMap<'a> {
        let head = Box::new_in(
            Node {
                key: arena.alloc_slice_copy(&[]),
                value: arena.alloc_slice_copy(&[]),
                skips: vec![in arena; None; MAX_HEIGHT],
            },
            arena,
        );
        SkipMap {
            arena,
            head,
            rand: StdRng::seed_from_u64(0xdeadbeef),
            max_height: 1,
            len: 0,
        }
    }

    fn new_node(&mut self, height: usize, key: &[u8], value: &[u8]) -> *mut Node<'a> {
        let key = self.arena.alloc_slice_copy(key);
        let value = self.arena.alloc_slice_copy(value);

        self.arena.alloc(Node {
            key,
            value,
            skips: vec![in self.arena; None; height],
        })
    }

    fn random_height(&mut self) -> usize {
        let mut height = 1;
        while height < MAX_HEIGHT && self.rand.next_u32() % BRANCHING_FACTOR == 0 {
            height += 1;
        }
        assert!(height > 0);
        assert!(height <= MAX_HEIGHT);
        height
    }

    fn find_greater_or_equal(&self, key: &[u8]) -> Option<&'a Node<'a>> {
        let mut current = self.head.as_ref() as *const Node;
        let mut level = self.max_height - 1;

        loop {
            unsafe {
                let next = (*current).next(level);
                if let Some(next) = next {
                    match (*next).key.cmp(key) {
                        Ordering::Less => {
                            current = next;
                            continue;
                        }
                        Ordering::Equal => return next.as_ref(),
                        Ordering::Greater => {
                            if level == 0 {
                                return next.as_ref();
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
            if current == self.head.as_ref() {
                None
            } else if (*current).key.lt(key) {
                None
            } else {
                current.as_ref()
            }
        }
    }

    fn find_less_than(&self, key: &[u8]) -> Option<&'a Node<'a>> {
        let mut current = self.head.as_ref() as *const Node;
        let mut level = self.max_height - 1;
        loop {
            unsafe {
                if let Some(next) = (*current).next(level) {
                    if (*next).key < key {
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
            if current == self.head.as_ref() {
                None
            } else if (*current).key.ge(key) {
                None
            } else {
                current.as_ref()
            }
        }
    }

    pub fn insert(&mut self, key: &[u8], value: &[u8]) {
        assert!(!key.is_empty());
        let mut prevs = std::vec![None; MAX_HEIGHT];
        let mut current = self.head.as_mut() as *mut Node;
        let mut level = self.max_height - 1;
        loop {
            unsafe {
                if let Some(next) = (*current).next(level) {
                    assert_ne!((*next).key, key, "duplicate entry");
                    if (*next).key.lt(key) {
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
            for prev in prevs.iter_mut().take(height).skip(current_height) {
                *prev = Some(self.head.as_mut())
            }
            self.max_height = height;
        }

        current = self.new_node(height, key, value);
        for (i, prev) in prevs.iter().flatten().enumerate().take(height) {
            unsafe {
                (*current).set_next(i, (**prev).next(i));
                (**prev).set_next(i, Some(current));
            }
        }

        self.len += 1;
    }

    pub fn contains(&self, key: &[u8]) -> bool {
        if let Some(node) = self.find_greater_or_equal(key) {
            unsafe { return node.key.eq(key) }
        }
        false
    }

    pub fn len(&self) -> usize {
        self.len
    }
}

struct SkipMapIterator<'a> {
    map: *const SkipMap<'a>,
    node: *const Node<'a>,
}

impl<'a> Iter for SkipMapIterator<'a> {
    type Item = (&'a [u8], &'a [u8]);

    fn valid(&self) -> bool {
        todo!()
    }

    fn prev(&mut self) {
        todo!()
    }

    fn next(&mut self) {
        todo!()
    }

    fn current(&self) -> Option<Self::Item> {
        todo!()
    }

    fn seek(&mut self, target: &[u8]) {
        todo!()
    }

    fn seek_to_first(&mut self) {
        todo!()
    }

    fn seek_to_last(&mut self) {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use crate::skiplist::SkipMap;
    use bumpalo::Bump;
    pub fn make_skipmap(arena: &Bump) -> SkipMap {
        let mut skm = SkipMap::new(arena);
        let keys = vec![
            "aba", "abb", "abc", "abd", "abe", "abf", "abg", "abh", "abi", "abj", "abk", "abl",
            "abm", "abn", "abo", "abp", "abq", "abr", "abs", "abt", "abu", "abv", "abw", "abx",
            "aby", "abz",
        ];

        for k in keys {
            skm.insert(k.as_bytes(), "def".as_bytes());
        }
        skm
    }

    #[test]
    fn test_insert() {
        let arena = Bump::new();
        let skm = make_skipmap(&arena);
        assert_eq!(skm.len(), 26);
        println!("{}", skm)
    }

    #[test]
    #[should_panic]
    fn test_no_dupes() {
        let arena = Bump::new();
        let mut skm = make_skipmap(&arena);
        // this should panic
        skm.insert("abc".as_bytes(), "def".as_bytes());
        skm.insert("abf".as_bytes(), "def".as_bytes());
    }

    #[test]
    fn test_find() {
        let arena = Bump::new();
        let skm = make_skipmap(&arena);
        assert_eq!(
            skm.find_greater_or_equal("abf".as_bytes()).unwrap().key,
            "abf".as_bytes()
        );
        assert!(skm
            .find_greater_or_equal(&"ab{".as_bytes().to_vec())
            .is_none());
        assert_eq!(
            skm.find_greater_or_equal(&"aaa".as_bytes().to_vec())
                .unwrap()
                .key,
            "aba".as_bytes().to_vec()
        );
        assert_eq!(
            skm.find_greater_or_equal(&"ab".as_bytes()).unwrap().key,
            "aba".as_bytes()
        );
        assert_eq!(
            skm.find_greater_or_equal(&"abc".as_bytes()).unwrap().key,
            "abc".as_bytes()
        );
        assert!(skm.find_less_than(&"ab0".as_bytes()).is_none());
        assert_eq!(
            skm.find_less_than(&"abd".as_bytes()).unwrap().key,
            "abc".as_bytes()
        );
        assert_eq!(
            skm.find_less_than(&"ab{".as_bytes()).unwrap().key,
            "abz".as_bytes()
        );
    }

    #[test]
    fn test_contains() {
        let arena = Bump::new();
        let mut skm = make_skipmap(&arena);
        assert!(skm.contains("aby".as_bytes()));
        assert!(skm.contains("abc".as_bytes()));
        assert!(skm.contains("abz".as_bytes()));
        assert!(!skm.contains("ab{".as_bytes()));
        assert!(!skm.contains("123".as_bytes()));
        assert!(!skm.contains("aaa".as_bytes()));
        assert!(!skm.contains("456".as_bytes()));
    }
}
