use crate::codec::decode_fixed32;
use crate::constant::{BLOCK_ENTRY_HEADER_SIZE, BLOCK_META_SIZE, CHECKSUM_SIZE};
use crate::iter::Iter;
use crate::table::{decode_key, decode_key_value};
use crate::table_index::BlockIndex;
use crate::Error;
use std::io::Write;

#[derive(Debug)]
pub(crate) struct Block<'a> {
    pub(crate) block_offset: usize,
    pub(crate) data: &'a [u8],
    pub(crate) entry_offsets: Vec<usize>,
    pub(crate) checksum: u32,
}

impl<'a> Block<'a> {
    pub(crate) fn open(block_offset: usize, data: &'a [u8]) -> Self {
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
            block_offset,
            data,
            entry_offsets,
            checksum,
        }
    }

    pub(crate) fn get(&'a self, key: &[u8]) -> Option<&'a [u8]> {
        match self.get_value_offset(key) {
            None => None,
            Some((start, end)) => Some(&self.data[start..end]),
        }
    }

    pub(crate) fn get_value_offset(&'a self, key: &[u8]) -> Option<(usize, usize)> {
        self.entry_offsets
            .binary_search_by(|entry_offset| {
                let entry_key = decode_key(&self.data[*entry_offset..]);
                entry_key.cmp(key)
            })
            .map(|index| {
                let entry_offset = self.entry_offsets[index];
                let key_size = decode_fixed32(&self.data[entry_offset..entry_offset + 4]) as usize;
                let value_size =
                    decode_fixed32(&self.data[entry_offset + 4..entry_offset + 8]) as usize;
                let value_offset_start = BLOCK_ENTRY_HEADER_SIZE + key_size;
                let value_offset_end = value_offset_start + value_size;
                Some((value_offset_start, value_offset_end))
            })
            .unwrap_or_default()
    }

    pub(crate) fn get_value_offset_abs(&'a self, key: &[u8]) -> Option<(usize, usize)> {
        self.get_value_offset(key)
            .map(|(start, end)| (start + self.block_offset, end + self.block_offset))
    }

    pub(crate) fn size(&self) -> usize {
        self.entry_offsets.len()
    }

    pub(crate) fn into_iter(self) -> BlockIterator<'a> {
        let cursor = 0;
        let entry_offset = self.entry_offsets[cursor as usize];
        let item = Some(decode_key_value(&self.data[entry_offset..]));
        BlockIterator {
            cursor,
            block: self,
            item,
        }
    }
}

#[derive(Debug)]
pub(crate) struct BlockIterator<'a> {
    cursor: isize,
    block: Block<'a>,
    item: Option<(&'a [u8], &'a [u8])>,
}
impl<'a> BlockIterator<'a> {
    fn reset(&mut self) {
        self.item = self
            .block
            .entry_offsets
            .get(self.cursor as usize)
            .map(|entry_offset| decode_key_value(&self.block.data[*entry_offset..]));
    }
}
impl<'a> Iter for BlockIterator<'a> {
    type Item = (&'a [u8], &'a [u8]);

    fn valid(&self) -> bool {
        if self.cursor < 0 || self.cursor > self.block.entry_offsets.len() as isize - 1 {
            return false;
        }
        true
    }

    fn prev(&mut self) {
        if !self.valid() {
            return;
        }
        self.reset();
        self.cursor -= 1;
    }

    fn next(&mut self) {
        if !self.valid() {
            return;
        }
        self.reset();
        self.cursor += 1;
    }

    fn current(&self) -> Option<Self::Item> {
        self.item
    }

    fn seek(&mut self, target: &[u8]) {
        self.cursor = match self.block.entry_offsets.binary_search_by(|entry_offset| {
            let entry_key = decode_key(&self.block.data[*entry_offset..]);
            entry_key.cmp(target)
        }) {
            Ok(index) => index as isize,
            Err(index) => index as isize,
        };
        self.reset();
    }

    fn seek_to_first(&mut self) {
        self.cursor = (*self.block.entry_offsets.first().unwrap_or(&0)) as isize;
        self.reset();
    }

    fn seek_to_last(&mut self) {
        self.cursor = (*self.block.entry_offsets.last().unwrap_or(&0)) as isize;
        self.reset();
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
            entries_offset: 0,
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
