use crate::codec::Codec;
use crate::skiplist::SkipList;
use crate::types::{MemEntry, ValueType};

pub(crate) struct MemTable {
    table : SkipList
}


impl MemTable {

    pub(crate) fn add(&mut self,seq : u64, vtype : ValueType, key: &[u8], value: &[u8]) -> crate::Result<()> {
        let entry = MemEntry::new(seq,vtype,key, value).encode()?;

    }


    pub(crate) fn approximate_memory_usage(&self) -> usize {
        self.table.allocated_bytes()
    }
}
