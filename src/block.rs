use crate::bloom::BloomFilterPolicy;
use crate::codec::decode_fixed32;
use crate::constant::*;
use crate::{bloom, table_index};
use bumpalo::Bump;
use std::io::Write;
use std::rc::Rc;
use std::sync::Arc;
use crate::table::{decode_key, decode_key_value, TableOptions};
use crate::table_index::BlockIndex;



#[derive(Debug)]
pub(crate) struct Block<'a> {
    pub(crate) index : BlockIndex<'a>,
    pub(crate) data: &'a [u8],
    pub(crate) entry_offsets: Vec<usize>,
    pub(crate) checksum: u32,
}

impl<'a> Block<'a> {
    pub(crate) fn open(index : BlockIndex<'a>, data: &'a [u8]) -> Self {
        let checksum = decode_fixed32(&data[data.len() - CHECKSUM_SIZE..data.len()]);
        let block_meta_size = decode_fixed32(
            &data[data.len() - CHECKSUM_SIZE - BLOCK_META_SIZE..data.len() - CHECKSUM_SIZE],
        ) as usize;
        let block_meta_offset =
            data.len() - CHECKSUM_SIZE - BLOCK_META_SIZE - (block_meta_size * 4);
        let block_metadata = &data[block_meta_offset..block_meta_offset + (block_meta_size * 4)];
        // TODO check checksum
        let entry_offsets: Vec<_> = block_metadata
            .chunks_exact(4)
            .map(|k| decode_fixed32(k) as usize)
            .collect();
        Self {
            index,
            data,
            entry_offsets,
            checksum,
        }
    }

    pub(crate) fn get(&'a self, key : &[u8]) -> Option<&'a [u8]> {
        match self.get_value_offset(key) {
            None => {
                None
            }
            Some((start,end)) => {
                Some(&self.data[start..end])
            }
        }
    }

    pub(crate) fn get_value_offset(&'a self, key : &[u8]) -> Option<(usize,usize)> {
        self.entry_offsets.binary_search_by(|entry_offset| {
            let entry_key = decode_key(&self.data[*entry_offset..]);
            entry_key.cmp(key)
        }).map(|index| {
            let entry_offset = self.entry_offsets[index];
            let key_size = decode_fixed32(&self.data[entry_offset..entry_offset + 4]) as usize;
            let value_size = decode_fixed32(&self.data[entry_offset + 4..entry_offset + 8]) as usize;
            let value_offset_start = BLOCK_ENTRY_HEADER_SIZE + key_size;
            let value_offset_end = value_offset_start + value_size;
            Some((value_offset_start, value_offset_end))
        }).unwrap_or_default()
    }

    pub(crate) fn get_value_offset_abs(&'a self, key : &[u8]) -> Option<(usize,usize)> {
        self.get_value_offset(key).map(|(start, end)| {
            (start + self.index.offset_start, end + self.index.offset_start)
        })
    }
}

pub(crate) struct BlockBuilder {
    pub(crate) data: Vec<u8>,
    pub(crate) base_key: Vec<u8>,
    pub(crate) entry_offsets: Vec<u32>,
    pub(crate) key_hashes: Vec<u32>,
    pub(crate) entry_count: usize,
    pub(crate) entries_offset: u32,
}

impl BlockBuilder {
    pub(crate) fn new() -> BlockBuilder {
        Self {
            data: vec![],
            base_key: vec![],
            entry_offsets: vec![],
            key_hashes: vec![],
            entry_count: 0,
            entries_offset: 0
        }
    }

    pub(crate) fn finish<W: Write>(&self, dst: &mut W) -> crate::Result<usize> {
        let mut written_bytes = 0_usize;
        let crc = crc32fast::hash(self.data.as_slice());
        written_bytes += dst.write(self.data.as_slice())?;

        let offset_count = self.entry_offsets.len();
        for offset in &self.entry_offsets {
            written_bytes += dst.write(offset.to_le_bytes().as_slice())?;
        }
        written_bytes += dst.write(&(offset_count as u32).to_le_bytes())?;
        written_bytes += dst.write(&crc.to_le_bytes())?;
        Ok(written_bytes)
    }
}

impl Default for BlockBuilder {
    fn default() -> Self {
        BlockBuilder::new()
    }
}
