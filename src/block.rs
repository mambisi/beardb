use std::io::Write;
use crate::codec::decode_fixed32;
use crate::constant::{BLOCK_ENTRY_HEADER_SIZE, BLOCK_META_SIZE, CHECKSUM_SIZE};
use crate::table::{decode_key, decode_key_value, TableOptions};
use crate::{ensure, Error};

#[derive(Debug)]
pub(crate) struct Block<'a> {
    pub(crate) block_offset: usize,
    pub(crate) data: &'a [u8],
    pub(crate) entry_offsets: Vec<usize>,
    pub(crate) checksum: u32,
}

#[derive(Debug)]
pub(crate) struct OwnedBlock {
    pub(crate) block_offset: usize,
    pub(crate) data: Vec<u8>,
    pub(crate) entry_offsets: Vec<usize>,
    pub(crate) checksum: u32,
}

impl<'a> Block<'a> {
    pub(crate) fn open(
        block_offset: usize,
        data: &'a [u8],
        opts: &TableOptions,
    ) -> crate::Result<Self> {
        let checksum = decode_fixed32(&data[data.len() - CHECKSUM_SIZE..data.len()]);
        if opts.checksum {
            let cal_checksum = crc32fast::hash(&data[..data.len() - CHECKSUM_SIZE]);
            ensure!(
                cal_checksum == checksum,
                Error::Corruption("checksum failed".to_string())
            );
        }
        let block_meta_size = decode_fixed32(
            &data[data.len() - CHECKSUM_SIZE - BLOCK_META_SIZE..data.len() - CHECKSUM_SIZE],
        ) as usize;
        let block_meta_offset =
            data.len() - CHECKSUM_SIZE - BLOCK_META_SIZE - (block_meta_size * 4);
        let block_metadata = &data[block_meta_offset..block_meta_offset + (block_meta_size * 4)];
        let entry_offsets: Vec<_> = block_metadata
            .chunks_exact(4)
            .map(|k| decode_fixed32(k) as usize)
            .collect();
        Ok(Self {
            block_offset,
            data,
            entry_offsets,
            checksum,
        })
    }

    pub(crate) fn get(&'a self, key: &[u8]) -> Option<&'a [u8]> {
        match self.get_value_offset(key) {
            None => None,
            Some((start, end)) => Some(&self.data[start..end]),
        }
    }

    pub(crate) fn get_value_offset(&self, key: &[u8]) -> Option<(usize, usize)> {
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

    pub(crate) fn get_value_offset_abs(&self, key: &[u8]) -> Option<(usize, usize)> {
        self.get_value_offset(key)
            .map(|(start, end)| (start + self.block_offset, end + self.block_offset))
    }

    pub(crate) fn size(&self) -> usize {
        self.entry_offsets.len()
    }

    fn to_owned(self) -> OwnedBlock {
        OwnedBlock {
            block_offset: self.block_offset,
            data: self.data.to_vec(),
            entry_offsets: self.entry_offsets,
            checksum: self.checksum,
        }
    }
    pub(crate) fn into_iter(self) -> BlockIterator {
        let cursor = 0;
        BlockIterator {
            cursor,
            block: self.to_owned(),
        }
    }
}

#[derive(Debug)]
pub(crate) struct BlockIterator {
    cursor: isize,
    block: OwnedBlock,
}

impl BlockIterator {
    pub(crate) fn valid(&self) -> bool {
        if self.cursor < 0 || self.cursor > self.block.entry_offsets.len() as isize - 1 {
            return false;
        }
        true
    }

    pub(crate) fn prev(&mut self) {
        if !self.valid() {
            return;
        }
        self.cursor -= 1;
    }

    pub(crate) fn next(&mut self) {
        if !self.valid() {
            return;
        }
        self.cursor += 1;
    }

    pub(crate) fn current(&self) -> Option<(&[u8], &[u8])> {
        self.block
            .entry_offsets
            .get(self.cursor as usize)
            .map(|entry_offset| decode_key_value(&self.block.data[*entry_offset..]))
    }

    pub(crate) fn seek(&mut self, target: &[u8]) {
        self.cursor = match self.block.entry_offsets.binary_search_by(|entry_offset| {
            let entry_key = decode_key(&self.block.data[*entry_offset..]);
            entry_key.cmp(target)
        }) {
            Ok(index) => index as isize,
            Err(index) => index as isize,
        };
    }

    pub(crate) fn seek_to_first(&mut self) {
        self.cursor = (*self.block.entry_offsets.first().unwrap_or(&0)) as isize;
    }

    pub(crate) fn seek_to_last(&mut self) {
        self.cursor = (*self.block.entry_offsets.last().unwrap_or(&0)) as isize;
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

    pub(crate) fn finish<W: Write>(&mut self, dst: &mut W) -> crate::Result<usize> {
        let offset_count = self.entry_offsets.len();
        for offset in &self.entry_offsets {
            self.data.extend_from_slice(offset.to_le_bytes().as_slice());
        }
        self.data
            .extend_from_slice(&(offset_count as u32).to_le_bytes());
        let crc = crc32fast::hash(self.data.as_slice());

        let written_bytes = dst.write(self.data.as_slice())? + dst.write(&crc.to_le_bytes())?;
        self.data.clear();
        self.base_key.clear();
        self.entry_offsets.clear();
        self.key_hashes.clear();
        self.entry_count = 0;
        self.entries_offset = 0;

        Ok(written_bytes)
    }
}

impl Default for BlockBuilder {
    fn default() -> Self {
        BlockBuilder::new()
    }
}
