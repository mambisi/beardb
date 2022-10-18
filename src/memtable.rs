use crate::cmp::Comparator;
use crate::codec::Codec;
use crate::skiplist::SkipList;
use crate::types::{MemEntry, ValueType};
use std::cmp::Ordering;
use std::sync::Arc;

pub(crate) struct MemTable {
    cmp: Arc<Box<dyn Comparator>>,
    table: SkipList,
}

impl MemTable {
    pub(crate) fn add(
        &self,
        seq: u64,
        vtype: ValueType,
        key: &[u8],
        value: &[u8],
    ) -> crate::Result<()> {
        let entry = MemEntry::new(seq, vtype, key, value).encode()?;
        self.table.insert(&entry)
    }

    pub(crate) fn get(&self, key: &[u8]) -> crate::Result<Option<&[u8]>> {
        let mut iter = self.table.iter();
        iter.seek(key)?;

        if !iter.valid() {
            return Ok(None);
        }
        if let Some(raw_memkey) = iter.current()? {
            let memkey = MemEntry::decode_from_slice(raw_memkey)?;
            if memkey.key().cmp(key) == Ordering::Equal {
                return match memkey.value_type() {
                    ValueType::Deletion => Ok(None),
                    ValueType::Value => Ok(Some(memkey.value())),
                };
            }
        }
        return Ok(None);
    }

    pub(crate) fn approximate_memory_usage(&self) -> usize {
        self.table.allocated_bytes()
    }
}
