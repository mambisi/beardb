use crate::codec::Codec;
use crate::skiplist::SkipList;
use crate::types::{MemEntry, ValueType};

pub(crate) struct MemTable {
    table : SkipList
}


impl MemTable {

    pub(crate) fn add(&self,seq : u64, vtype : ValueType, key: &[u8], value: &[u8]) -> crate::Result<()> {
        let entry = MemEntry::new(seq,vtype,key, value).encode()?;
        self.table.insert(&entry)
    }

    pub(crate) fn get(&self,  key: &[u8]) -> crate::Result<Option<&[u8]>> {
        todo!()
    }


    pub(crate) fn approximate_memory_usage(&self) -> usize {
        self.table.allocated_bytes()
    }
}
