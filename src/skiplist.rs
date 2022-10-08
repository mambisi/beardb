use std::borrow::BorrowMut;
use std::cell::RefCell;
use std::fmt::{Debug, Formatter};
use std::ops::Deref;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicI32, AtomicPtr, AtomicU32, AtomicUsize, Ordering};
use bumpalo::{Bump};
use rand::prelude::StdRng;
use rand::{RngCore, SeedableRng};


const MAX_HEIGHT: usize = 12;
const BRANCHING_FACTOR: u32 = 4;

#[derive(Clone)]
struct SkipListNode {
    key: Vec<u8>,
    value: Vec<u8>,
    next: Vec<Option<*mut SkipListNode>>,
}

impl SkipListNode {
    fn next(&self, n: usize) -> Option<*mut SkipListNode> {
        self.next[n].clone()
    }

    fn set_next(&mut self, n: usize, node: Option<*mut SkipListNode>) {
        self.next[n] = node
    }
}

pub struct SkipList {
    arena : Bump,
    head: Box<SkipListNode>,
    rand: StdRng,
    maxheight: usize,
}

impl SkipList {
    pub fn new() -> SkipList {
        let arena =  Bump::new();
        let head = Box::new(SkipListNode {
            key: vec![],
            value: vec![],
            next: vec![None; MAX_HEIGHT],
        });
        SkipList {
            arena,
            head,
            rand: StdRng::seed_from_u64(0xdeadbeef),
            maxheight: 1,
        }
    }

    fn new_node(&self, height: usize, key: Vec<u8>, value: Vec<u8>) -> *mut SkipListNode {
        self.arena.alloc(SkipListNode {
            key,
            value,
            next: vec![None; height],
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
            unsafe { return (*node).key.as_slice() < key; }
        }
        return false
    }

    fn find_greater_or_equal(&mut self, key: &[u8], mut prevs: Option<&mut Vec<Option<*mut SkipListNode>>>) -> Option<*mut SkipListNode> {
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
                        return next
                    } else {
                        level -= 1;
                    }
                }
            }
        }
    }

    pub fn insert(&mut self, key: Vec<u8>, value: Vec<u8>) {
        let mut prevs = vec![None; MAX_HEIGHT];
        let mut current = self.find_greater_or_equal(&key, Some(&mut prevs));
        if let Some(current) = current {
            unsafe { assert!((*current).key.ne(&key)) }
        }
        let height = self.random_height();
        if height > self.maxheight {
            for i in self.maxheight..height {
                prevs[i] = Some(self.head.as_mut())
            }
            self.maxheight = height;
        }
        let mut current =  self.new_node(height, key, value);
        for i in 0..height{
            unsafe {
                if let Some(prev) = prevs[i] {
                    (*current).set_next(i, (*prev).next(i));
                    (*prev).set_next(i, Some(current));
                }
            }
        }
    }

    pub  fn dbg_print(&self) {
        unsafe {
            let mut current = self.head.as_ref() as *const SkipListNode;
            loop {
                eprintln!(
                    "{:?} {:?}/{:?} - {:?}",
                    current,
                    (*current).key,
                    (*current).value,
                    (*current).next
                );
                let next = (*current).next(0);
                if let Some(next) = next{
                    current = next;
                }else {
                    break
                }

            }
        }
    }

}

#[cfg(test)]
mod tests {
    use crate::skiplist::SkipList;

    #[test]
    fn randoms() {
        let mut list = SkipList::new();
        for i in 0..100 {
           println!("{}",  list.random_height())
        }
    }
}