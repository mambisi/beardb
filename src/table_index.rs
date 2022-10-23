use crate::bloom::BloomFilterPolicy;
use crate::codec::Reader;
use crate::constant::BLOCK_ENTRY_HEADER_SIZE;
use crate::table::TableOptions;
use crate::{codec, Error};
use bytecheck::CheckBytes;
use core::slice::SlicePattern;
use rkyv::vec::ArchivedVec;
use rkyv::{AlignedVec, Archive, Archived, Deserialize, Serialize};
use std::cmp::Ordering;
use std::io::Write;
use std::slice::Iter as SliceIter;
use std::sync::Arc;

type BloomFilter = Vec<u8>;

#[derive(Archive, Deserialize, Serialize, Debug, PartialEq)]
#[archive(compare(PartialEq))]
#[archive_attr(derive(CheckBytes, Debug))]
struct BlockOffsetsIndex {
    base_key: Vec<u8>,
    filter: BloomFilter,
    offset_start: u32,
    offset_end: u32,
}

#[derive(Archive, Deserialize, Serialize, Debug, PartialEq)]
#[archive(compare(PartialEq))]
#[archive_attr(derive(CheckBytes, Debug))]
struct TableIndex {
    offsets: Vec<BlockOffsetsIndex>,
    key_count: u32,
}

const INDEX_BLOCK_SIZE: usize = 4 * 1024;

pub(crate) struct TableIndexBuilder {
    inner: TableIndex,
}

impl TableIndexBuilder {
    fn create_table_offsets(
        blocks: &Vec<u8>,
        index: &Vec<u32>,
        bloom_filter: Vec<BloomFilter>,
    ) -> Vec<BlockOffsetsIndex> {
        let mut offsets = Vec::new();

        for (i, (start, filter)) in index.iter().zip(bloom_filter).enumerate() {
            let array = &blocks[(*start as usize)..];
            let key_size = codec::decode_fixed32(&array[..4]) as usize;
            let base_key =
                Vec::from(&array[BLOCK_ENTRY_HEADER_SIZE..BLOCK_ENTRY_HEADER_SIZE + key_size]);
            let offset_start = *start;
            let offset_end = match index.get(i + 1) {
                None => (blocks.len() - 1) as u32,
                Some(n) => *n,
            };
            let index = BlockOffsetsIndex {
                base_key,
                filter,
                offset_start,
                offset_end,
            };
            offsets.push(index)
        }
        offsets
    }
    pub(crate) fn new(
        blocks: &Vec<u8>,
        index: &Vec<u32>,
        bloom_filter: Vec<BloomFilter>,
        key_count: u32,
    ) -> Self {
        Self {
            inner: TableIndex {
                offsets: TableIndexBuilder::create_table_offsets(blocks, index, bloom_filter),
                key_count,
            },
        }
    }
    pub(crate) fn finish<'a>(self) -> crate::Result<AlignedVec> {
        let bytes = rkyv::to_bytes::<_, INDEX_BLOCK_SIZE>(&self.inner)
            .map_err(|e| Error::AnyError(Box::new(e)))?;
        Ok(bytes)
    }
}

#[derive(Debug)]
pub(crate) struct TableIndexReader<'a> {
    inner: &'a ArchivedTableIndex,
    opts: Arc<TableOptions>,
}

#[derive(Debug, Copy, Clone)]
pub(crate) struct BlockIndex<'a> {
    pub(crate) base_key: &'a [u8],
    pub(crate) bloomfilter: &'a [u8],
    pub(crate) offset_start: usize,
    pub(crate) offset_end: usize,
}

impl<'a> TableIndexReader<'a> {
    pub(crate) fn open(
        data: &'a [u8],
        opts: Arc<TableOptions>,
    ) -> crate::Result<TableIndexReader<'a>> {
        let inner = rkyv::check_archived_root::<TableIndex>(data)
            .map_err(|err| Error::Corruption(format!("{}", err)))?;
        Ok(Self { inner, opts })
    }

    pub(crate) fn get_block_index(&self, at: usize) -> Option<BlockIndex> {
        match self.inner.offsets.get(at) {
            Some(index) => Some(BlockIndex {
                base_key: index.base_key.as_slice(),
                bloomfilter: index.filter.as_slice(),
                offset_start: index.offset_start as usize,
                offset_end: index.offset_end as usize,
            }),
            _ => None,
        }
    }

    pub(crate) fn iter(&self) -> TableIndexIterator {
        TableIndexIterator {
            inner: self.inner.offsets.iter(),
        }
    }

    pub(crate) fn blocks_count(&self) -> usize {
        self.inner.offsets.len()
    }

    pub(crate) fn keys_count(&self) -> usize {
        self.inner.key_count as usize
    }

    pub(crate) fn find_key_block(&self, key: &[u8]) -> Option<BlockIndex> {
        let pos = self
            .inner
            .offsets
            .partition_point(|t| key.ge(t.base_key.as_slice()));
        let index = &self.inner.offsets[pos - 1];
        let block_index: BlockIndex = BlockIndex {
            base_key: index.base_key.as_slice(),
            bloomfilter: index.filter.as_slice(),
            offset_start: index.offset_start as usize,
            offset_end: index.offset_end as usize,
        };
        if self.opts.policy.key_and_match(key, block_index.bloomfilter) {
            return Some(block_index);
        }
        return None;
    }

    pub(crate) fn contains_key(&self, key: &[u8]) -> bool {
        self.find_key_block(key).is_some()
    }
}

#[derive(Debug)]
pub(crate) struct TableIndexIterator<'a> {
    inner: SliceIter<'a, ArchivedBlockOffsetsIndex>,
}

impl<'a> Iterator for TableIndexIterator<'a> {
    type Item = BlockIndex<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|index| BlockIndex {
            base_key: index.base_key.as_slice(),
            bloomfilter: index.filter.as_slice(),
            offset_start: index.offset_start as usize,
            offset_end: index.offset_start as usize,
        })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}
