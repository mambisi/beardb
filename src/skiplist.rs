use std::cell::RefCell;
use std::ops::Deref;
use std::ptr::NonNull;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicI32, AtomicPtr, AtomicU32, AtomicUsize, Ordering};
use bumpalo::{Bump};
use rand::prelude::StdRng;
use rand::{RngCore, SeedableRng};


const MAX_HEIGHT: usize = 12;
const BRANCHING_FACTOR: u32 = 4;

#[derive(Clone)]
struct SkipListNode<'a> {
    key: &'a [u8],
    value: &'a [u8],
    next: *mut Vec<Arc<AtomicPtr<SkipListNode<'a>>>>,
}

impl<'a> SkipListNode<'a> {

    unsafe fn next(&'a self, n: usize) -> *mut SkipListNode {
        match (*self.next).get(n) {
            None => {
                std::ptr::null_mut()
            }
            Some(prt) => {
                prt.load(Ordering::Acquire)
            }
        }
    }

    unsafe fn set_next(&'a self, n: usize, node: *mut SkipListNode<'a>) {
        (*self.next)[n].store(node, Ordering::Release)
    }

    unsafe fn nb_next(&'a self, n: usize) -> *mut SkipListNode {
        match (*self.next).get(n) {
            None => {
                std::ptr::null_mut()
            }
            Some(prt) => {
                prt.load(Ordering::Relaxed)
            }
        }
    }

    unsafe fn nb_set_next(&'a self, n: usize, node: *mut SkipListNode<'a>) {
        (*self.next)[n].store(node, Ordering::Relaxed)
    }
}


pub struct SkipList<'a> {
    arena: Bump,
    head: *mut SkipListNode<'a>,
    rand : Rc<RefCell<StdRng>>,
    maxheight: AtomicUsize,
}

impl<'a> SkipList<'a> {
    pub fn new() -> SkipList<'a> {
        let arena = Bump::new();
        let next = arena.alloc(vec![Arc::new(AtomicPtr::new(std::ptr::null_mut())); MAX_HEIGHT]);
        let node = arena.alloc(SkipListNode {
            key: &[],
            value: &[],
            next,
        });
        let head = node as *mut SkipListNode;
        Self {
            arena,
            head,
            rand: Rc::new(RefCell::new(StdRng::seed_from_u64(0xdeadbeef))),
            maxheight: AtomicUsize::new(1),
        }
    }

    fn new_node(&'a self, height: usize, key: Vec<u8>, value: Vec<u8>) -> &'a mut SkipListNode {
        let key = self.arena.alloc_slice_copy(&key);
        let value = self.arena.alloc_slice_copy(&value);
        let next = self.arena.alloc(vec![Arc::new(AtomicPtr::new(std::ptr::null_mut())); height]);
        let node = self.arena.alloc(SkipListNode {
            key,
            value,
            next,
        });
        node
    }


    fn random_height(&'a self) -> usize {
        let mut height = 1;
        let mut rng = self.rand.borrow_mut();
        while height < MAX_HEIGHT && rng.next_u32() % BRANCHING_FACTOR == 0 {
            height += 1;
        }
        assert!(height > 0);
        assert!(height <= MAX_HEIGHT);
        height
    }

    fn key_is_after_node(&'a self, key: &[u8], node: *mut SkipListNode) -> bool {
        if node.is_null() {
            println!("key_is_after_node node.is_null");
            return false;
        }
        unsafe {
            println!("key_is_after_node {} {:?}-{:?}", (*node).key.lt(key), (*node).key, key);
            return (*node).key.lt(key);
        }
    }

    fn find_greater_or_equal(&'a self, key: &[u8], mut prevs: *mut Vec<u32>) -> *mut SkipListNode {
        let mut current = self.head;
        let mut level = self.get_max_height();
        loop {
            unsafe {
                let next = (*current).next(level);
                if self.key_is_after_node(key, next) {
                    if (next as u32) == (current as u32) {
                        panic!("cycling")
                    }
                    current = next;
                } else {
                    if !prevs.is_null() {
                        (*prevs)[level] = current as u32
                    }
                    if level == 0 {
                        return next;
                    } else {
                        level -= 1;
                    }
                }
                println!("find_greater_or_equal DEBUG {}", level);
            }
        }
    }

    pub fn insert(&'a self, key: Vec<u8>, value: Vec<u8>) {
        unsafe {

            let prevs = self.arena.alloc(vec![0; MAX_HEIGHT]);
            println!("Debug 1 {}", self.get_max_height());
            let mut current = self.find_greater_or_equal(&key, prevs);
            println!("Debug 2");
            assert!(current.is_null() || (*current).key.ne(key.as_slice()));
            let height = self.random_height();
            let current_height = self.get_max_height();

            println!("Debug 3 current_height {current_height} next height {height}");
            if height > self.get_max_height() {
                for i in current_height..height {
                    println!("Debug 3 set prevs");
                    prevs[i] = self.head as u32
                }

                self.maxheight.store(height, Ordering::Relaxed);
            }
            current = self.new_node(height, key, value);

            println!("==========================================================================================================================================================================================");
            for i in 0..height {

                eprintln!(
                    "prevs[{}] {:?} {:?}/{:?} - {:?}",
                    i,
                    prevs[i],
                    (*(prevs[i] as *mut SkipListNode) ).key,
                    (*(prevs[i] as *mut SkipListNode)).value,
                    (*(*(prevs[i] as *mut SkipListNode)).next)
                );
            }
            println!("==========================================================================================================================================================================================");

            for i in 0..height {
                println!("Debug 4");
                eprintln!(
                    "Current {:?} {:?}/{:?} - {:?}",
                    current,
                    (*current).key,
                    (*current).value,
                    (*(*current).next)
                );
                eprintln!(
                    "prevs[{}] {:?} {:?}/{:?} - {:?}",
                    i,
                    prevs[i],
                    (*(prevs[i] as *mut SkipListNode) ).key,
                    (*(prevs[i] as *mut SkipListNode)).value,
                    (*(*(prevs[i] as *mut SkipListNode)).next)
                );
                (*current).nb_set_next(i, (*(prevs[i] as *mut SkipListNode)).nb_next(i));
                eprintln!(
                    "(*current).nb_set_next(i, (*prevs[i]).nb_next(i))\nCurrent {:?} {:?}/{:?} - {:?}",
                    current,
                    (*current).key,
                    (*current).value,
                    (*(*current).next)
                );
                println!("Debug 5");
                (*(prevs[i] as *mut SkipListNode)).set_next(i, current);
                eprintln!(
                    "prevs[{}] {:?} {:?}/{:?} - {:?}",
                    i,
                    prevs[i],
                    (*(prevs[i] as *mut SkipListNode) ).key,
                    (*(prevs[i] as *mut SkipListNode)).value,
                    (*(*(prevs[i] as *mut SkipListNode)).next)
                );
            }
        }
    }

    fn get_max_height(&self) -> usize {
        self.maxheight.load(Ordering::Relaxed)
    }

    pub  fn dbg_print(&self) {
        unsafe {
            let mut current = self.head;
            loop {
                let next = (*current).next(0);
                if (next as u32) == (current as u32) {
                    break
                }
                if next.is_null() {
                    break
                }
                current = next;
                eprintln!(
                    "{:?} {:?}/{:?} - {:?}",
                    current,
                    (*current).key,
                    (*current).value,
                    (*current).next
                );
            }
        }
    }
}


#[cfg(test)]
mod tests {
    use crate::skiplist::SkipList;

    #[test]
    fn basic_test() {
        let map = SkipList::new();
        map.insert(vec![7, 5, 5], vec![3, 3, 3]);
        map.insert(vec![1, 2, 3], vec![1, 2, 3]);
        map.insert(vec![5, 5, 5], vec![5, 5, 5]);
        //map.insert(b"abc".to_vec(), b"cdb".to_vec());
    }
}

