use bumpalo::{boxed::Box, collections::Vec, vec, Bump};
use rand::prelude::StdRng;
use rand::{RngCore, SeedableRng};

const MAX_HEIGHT: usize = 12;
const BRANCHING_FACTOR: u32 = 4;

#[derive(Clone)]
struct SkipListNode<'a> {
    key: &'a [u8],
    value: &'a [u8],
    skips: Vec<'a, Option<*mut SkipListNode<'a>>>,
}

impl<'a> SkipListNode<'a> {
    fn next(&self, n: usize) -> Option<*mut SkipListNode<'a>> {
        self.skips[n]
    }

    fn set_next(&mut self, n: usize, node: Option<*mut SkipListNode<'a>>) {
        self.skips[n] = node
    }
}

pub struct SkipList<'a> {
    arena: &'a Bump,
    head: Box<'a, SkipListNode<'a>>,
    rand: StdRng,
    maxheight: usize,
    approx_mem: usize,
}

impl<'a> SkipList<'a> {
    pub fn new(arena: &'a Bump) -> SkipList<'a> {
        let head = Box::new_in(
            SkipListNode {
                key: arena.alloc_slice_copy(&[]),
                value: arena.alloc_slice_copy(&[]),
                skips: vec![in arena; None; MAX_HEIGHT],
            },
            arena,
        );
        SkipList {
            arena,
            head,
            rand: StdRng::seed_from_u64(0xdeadbeef),
            maxheight: 1,
            approx_mem: (2 * std::mem::size_of::<usize>())
                + std::mem::size_of::<std::boxed::Box<SkipListNode>>()
                + std::mem::size_of::<StdRng>()
                + MAX_HEIGHT * std::mem::size_of::<Option<*mut SkipListNode>>(),
        }
    }

    fn new_node(&mut self, height: usize, key: &[u8], value: &[u8]) -> *mut SkipListNode<'a> {
        let key = self.arena.alloc_slice_copy(key);
        let value = self.arena.alloc_slice_copy(value);

        self.arena.alloc(SkipListNode {
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

    fn key_is_after_node(&self, key: &[u8], node: Option<*mut SkipListNode>) -> bool {
        if let Some(node) = node {
            unsafe {
                return (*node).key < key;
            }
        }
        false
    }

    fn find_greater_or_equal(
        &mut self,
        key: &[u8],
        mut prevs: Option<&mut std::vec::Vec<Option<*mut SkipListNode<'a>>>>,
    ) -> Option<*mut SkipListNode<'a>> {
        let mut current = self.head.as_mut() as *mut SkipListNode;
        let mut level = self.maxheight - 1;
        unsafe {
            loop {
                let next = (*current).next(level);
                if self.key_is_after_node(key, next) {
                    current = next.unwrap();
                } else {
                    if let Some(prevs) = prevs.as_mut() {
                        prevs[level] = Some(current);
                    }
                    if level == 0 {
                        return next;
                    } else {
                        level -= 1;
                    }
                }
            }
        }
    }

    pub fn insert(&mut self, key: &[u8], value: &[u8]) {
        assert!(!key.is_empty());
        let mut prevs = std::vec![None; MAX_HEIGHT];
        let current = self.find_greater_or_equal(key, Some(&mut prevs));
        if let Some(current) = current {
            unsafe { assert!((*current).key.ne(key)) }
        }
        let height = self.random_height();
        let current_height = self.maxheight;
        if height > current_height {
            for prev in prevs.iter_mut().take(height).skip(current_height) {
                *prev = Some(self.head.as_mut())
            }
            self.maxheight = height;
        }
        let current = self.new_node(height, key, value);
        for (i, prev) in prevs.iter().flatten().enumerate() {
            unsafe {
                (*current).set_next(i, (*(*prev)).next(i));
                (*(*prev)).set_next(i, Some(current));
            }
        }

        unsafe {
            let added_mem = std::mem::size_of::<SkipListNode>()
                + std::mem::size_of::<Option<*mut SkipListNode>>() * (*current).skips.len()
                + (*current).key.len()
                + (*current).value.len();
            self.approx_mem += added_mem;
        }
    }

    pub fn dbg_print(&self) {
        unsafe {
            let mut current = self.head.as_ref() as *const SkipListNode;
            loop {
                eprintln!(
                    "{:?} {:?}/{:?} - {:?}",
                    current,
                    (*current).key,
                    (*current).value,
                    (*current).skips
                );
                let next = (*current).next(0);
                if let Some(next) = next {
                    current = next;
                } else {
                    break;
                }
            }

            println!("approx mem {}", self.approx_mem);
            println!("arena mem {}", self.arena.allocated_bytes())
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::skiplist::SkipList;
    use bumpalo::Bump;

    #[test]
    fn randoms() {
        let arena = Bump::new();
        let mut list = SkipList::new(&arena);
        for i in 0..100 {
            println!("{}", list.random_height())
        }
    }
}
