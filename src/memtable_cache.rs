use std::sync::{Arc, Mutex};
use crate::lfu_cache::LFUCache;
use crate::memtable::MemTable;
use crate::types::CacheKey;

pub(crate) struct MemtableCache {
    cache : Mutex<LFUCache<CacheKey, Arc<MemTable>>>
}

impl MemtableCache {

}