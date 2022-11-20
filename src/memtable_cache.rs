use core::slice::SlicePattern;
use std::sync::{Arc, Mutex};

use bytes::BytesMut;

use rcache::Cache;

use crate::codec::{Codec, decode_fixed64};
use crate::iter::Iter;
use crate::memtable::MemTable;
use crate::table::Table;
use crate::types::{CacheKey, MemEntry};

pub(crate) struct MemtableCache {
    cache: Cache<CacheKey, Arc<MemTable>>,
}

impl MemtableCache {
    pub(crate) fn insert(&self, table: Table) -> crate::Result<()> {
        let mut table_iter = table.iter()?;
        let memtable = MemTable::new();
        while table_iter.valid() {
            let Some((key, value)) = table_iter.current() else {
                break
            };

            let raw_entry = [key.as_ref(), value.as_ref()].concat();
            memtable.add_raw(&raw_entry)?;
            table_iter.next();
        }

        Ok(())
    }
}
