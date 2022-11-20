use std::fs::File;
use std::sync::Arc;
use std::time::Instant;

use bytes::BytesMut;
use parking_lot::RwLock;

use rcache::bloom::Bloom;

use crate::codec::Codec;
use crate::log::LogWriter;
use crate::memtable_cache::MemtableCache;
use crate::table::Table;
use crate::types::{MemEntry, ValueType};

pub(crate) struct Chunk {
    id: u64,
    memcache: Arc<MemtableCache>,
    rebalance_lock: RwLock<()>,
    table: Table,
    log: File,
    bloomfilter: Bloom,
    sequence: Instant,
}


impl Chunk {
    pub(crate) fn put<K: AsRef<[u8]>, V: AsRef<[u8]>>(&self, key: K, value: V) -> crate::Result<()> {
        let _lock = self.rebalance_lock.try_write()?;
        let mut log_writer = LogWriter::new(&self.log);
        let entry = MemEntry::new(self.sequence.elapsed().as_nanos() as u64, ValueType::Value, key.as_ref(), value.as_ref());
        let encoded_entry = entry.encode()?;
        let _ = log_writer.add_record(&encoded_entry)?;
        let mut concat = BytesMut::from(key.as_ref());
        concat.extend_from_slice(value.as_ref());
        self.memcache.
            Ok(())
    }
}