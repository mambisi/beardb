use crate::skiplist::SkipList;

mod skiplist;

fn main() {
    let mut map = SkipList::new();
    map.insert(vec![7, 5, 5], vec![3, 3, 3]);
    map.insert(vec![1, 2, 3], vec![1, 2, 3]);
    map.insert(vec![5, 5, 5], vec![5, 5, 5]);
    map.insert(vec![0, 5, 5], vec![5, 5, 5]);
    map.insert(vec![0, 0, 5], vec![5, 5, 5]);
    map.insert(vec![0, 0, 1], vec![5, 5, 5]);
    map.dbg_print()
    // map.insert(vec![1, 0, 1], vec![1, 2, 3]);
    // map.dbg_print();
}