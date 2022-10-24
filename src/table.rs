use crate::block::{Block, BlockIterator};
use crate::bloom::BloomFilterPolicy;
use crate::codec::decode_fixed32;
use crate::constant::BLOCK_ENTRY_HEADER_SIZE;
use crate::iter::Iter;
use crate::table_index::{BlockIndex, TableIndexReader};
use crate::{codec, Error};
use core::slice::SlicePattern;
use memmap2::Mmap;
use std::marker::PhantomData;
use std::sync::Arc;
use std::thread::current;

pub(crate) fn decode_key(data: &[u8]) -> &[u8] {
    //TODO: Handle index errors
    let key_size = decode_fixed32(&data[..4]) as usize;
    &data[BLOCK_ENTRY_HEADER_SIZE..BLOCK_ENTRY_HEADER_SIZE + key_size]
}

pub(crate) fn decode_key_value(data: &[u8]) -> (&[u8], &[u8]) {
    //TODO: Handle index errors
    let key_size = decode_fixed32(&data[..4]) as usize;
    let value_size = decode_fixed32(&data[4..8]) as usize;
    let key = &data[BLOCK_ENTRY_HEADER_SIZE..BLOCK_ENTRY_HEADER_SIZE + key_size];
    let value =
        &data[BLOCK_ENTRY_HEADER_SIZE + key_size..BLOCK_ENTRY_HEADER_SIZE + key_size + value_size];
    (key, value)
}

pub(crate) enum CompressionType {
    Snappy,
    Zstd,
    Lz4,
}

#[derive(Debug)]
pub struct TableOptions {
    pub(crate) block_size: usize,
    pub(crate) table_size: usize,
    pub(crate) table_capacity: usize,
    pub(crate) checksum: bool,
    pub(crate) policy: BloomFilterPolicy,
}

impl Default for TableOptions {
    fn default() -> Self {
        Self {
            block_size: 4 * 1024,
            table_size: 2 << 20,
            table_capacity: ((2_u64 << 20_u64) as f64 / 0.9) as usize,
            checksum: true,
            policy: BloomFilterPolicy::new(10),
        }
    }
}

pub(crate) struct InnerTable {
    data: Mmap,
    id: u64,
    index_start: usize,
    index_len: usize,
    opts: Arc<TableOptions>,
}

impl InnerTable {
    fn open(id: u64, file: Mmap, opts: Arc<TableOptions>) -> InnerTable {
        let s = &file[file.as_slice().len() - 8..file.as_slice().len() - 4];
        let index_len = codec::decode_fixed32(s) as usize;
        let index_start = file.as_slice().len() - index_len - 8;
        InnerTable {
            data: file,
            id,
            index_start,
            index_len,
            opts,
        }
    }

    fn index(&self) -> crate::Result<TableIndexReader> {
        TableIndexReader::open(
            &self.data[self.index_start..(self.index_start + self.index_len)],
            self.opts.clone(),
        )
    }

    fn get_block(&self, index: usize) -> crate::Result<Option<Block>> {
        let index_reader = self.index()?;
        let block_index = match index_reader.get_block_index(index) {
            None => return Ok(None),
            Some(index) => index,
        };
        let raw_block = &self.data[block_index.offset_start..block_index.offset_end];
        let block = Block::open(block_index.offset_start, raw_block, self.opts.as_ref())?;
        return Ok(Some(block));
    }

    fn get(&self, key: &[u8]) -> crate::Result<Option<&[u8]>> {
        let index = self.index()?;
        let block_index = match index.find_key_block(key) {
            None => return Ok(None),
            Some(index) => index,
        };
        let raw_block = &self.data[block_index.offset_start..block_index.offset_end];
        let block = Block::open(block_index.offset_start, raw_block, self.opts.as_ref())?;
        if let Some((start, end)) = block.get_value_offset_abs(key) {
            return Ok(Some(&self.data[start..end]));
        }
        return Ok(None);
    }

    fn block_count(&self) -> usize {
        match self.index() {
            Ok(index) => index.blocks_count(),
            Err(_) => 0,
        }
    }
}

pub(crate) struct Table {
    inner: Arc<InnerTable>,
}

impl Table {
    fn open(id: u64, file: Mmap, opts: Arc<TableOptions>) -> Self {
        Self {
            inner: Arc::new(InnerTable::open(id, file, opts)),
        }
    }

    fn index(&self) -> crate::Result<TableIndexReader> {
        self.inner.index()
    }

    fn get_block(&self, index: usize) -> crate::Result<Option<Block>> {
        self.inner.get_block(index)
    }

    fn get(&self, key: &[u8]) -> crate::Result<Option<&[u8]>> {
        self.inner.get(key)
    }

    fn iter(&self) -> crate::Result<TableIterator> {
        let current = match self.get_block(0) {
            Ok(Some(c)) => Box::new(c.into_iter()),
            _ => return Err(Error::InvalidIterator),
        };
        Ok(TableIterator {
            cursor: 0,
            table: self.inner.clone(),
            current,
            error: None,
        })
    }
}

pub(crate) struct TableIterator {
    cursor: isize,
    table: Arc<InnerTable>,
    current: Box<BlockIterator>,
    error: Option<Error>,
}

impl TableIterator {
    fn reset(&mut self) {
        if !self.valid() {
            return;
        }
        match self.table.get_block(self.cursor as usize) {
            Ok(Some(c)) => {
                self.current = Box::new(c.into_iter());
            }
            _ => self.error = Some(Error::InvalidIterator),
        };
    }
}

impl Iter for TableIterator {
    type Item = (Box<[u8]>, Box<[u8]>);

    fn valid(&self) -> bool {
        if self.cursor < 0
            || self.cursor > self.table.block_count() as isize - 1
            || self.error.is_some()
        {
            return false;
        }
        true
    }

    fn prev(&mut self) {
        self.current.prev();
        if !self.current.valid() {
            self.cursor -= 1;
            self.reset();
            return;
        }
    }

    fn next(&mut self) {
        self.current.next();
        if !self.current.valid() {
            self.cursor += 1;
            self.reset();
            return;
        }
    }

    fn current(&self) -> Option<Self::Item> {
        self.current
            .current()
            .map(|(k, v)| (k.to_vec().into_boxed_slice(), v.to_vec().into_boxed_slice()))
    }

    fn seek(&mut self, target: &[u8]) {
        let index = match self.table.index() {
            Ok(index) => index,
            Err(err) => {
                self.error = Some(err);
                return;
            }
        };
        self.cursor = index.find_target_key_block(target) as isize;
        self.reset();
        self.current.seek(target)
    }

    fn seek_to_first(&mut self) {
        self.cursor = 0;
        self.reset();
    }

    fn seek_to_last(&mut self) {
        self.cursor = (self.table.block_count() - 1) as isize;
        self.reset()
    }
}

#[cfg(test)]
mod test {
    use crate::bloom::BloomFilterPolicy;
    use crate::iter::Iter;
    use crate::table::{InnerTable, Table, TableOptions};
    use crate::table_builder::TableBuilder;
    use core::slice::SlicePattern;
    use memmap2::{Mmap, MmapMut};
    use std::fs::OpenOptions;
    use std::sync::Arc;
    use tempfile::tempfile;

    fn table_opts() -> Arc<TableOptions> {
        Arc::new(TableOptions {
            block_size: 11 * 5,
            table_size: 2 << 20,
            table_capacity: ((2_u64 << 20_u64) as f64 / 0.9) as usize,
            checksum: true,
            policy: BloomFilterPolicy::new(10),
        })
    }

    #[test]
    fn basic_test_more_data() {
        let mut file = tempfile().unwrap();
        //let mut dst = unsafe { MmapMut::map_mut(&file).unwrap() };
        let data = vec![
            "aba", "abb", "abc", "abd", "abe", "abf", "abg", "abh", "abi", "abj", "abk", "abl",
            "abm", "abn", "abo", "abp", "abq", "abr", "abs", "abt", "abu", "abv", "abw", "abx",
            "aby", "abz",
        ];

        let opts = table_opts();
        let mut table = TableBuilder::new_with_options(&mut file, opts.clone());

        for item in data {
            table.add(item.as_bytes(), item.as_bytes()).unwrap();
        }

        table.finish().unwrap();
        //file.sync_all().unwrap();

        let data = unsafe { Mmap::map(&file).unwrap() };
        let table = InnerTable::open(12, data, opts.clone());
        let index = table.index().unwrap();
        println!("block index {:?}", index.find_key_block(&[97, 98, 113]));
        println!("---------------------------------------------------------------");
        println!("block index {:?}", index.find_key_block(b"abr"));
        println!("Value {:?}", table.get(b"abr"));
        println!("---------------------------------------------------------------");
        println!("block index {:?}", index.find_key_block(b"aba"));
        println!("Value {:?}", table.get(b"aba"));
        println!("---------------------------------------------------------------");
        println!("block index {:?}", index.find_key_block(b"zzz"));

        println!("---------------------------------------------------------------");
        let block = table.get_block(0).unwrap().unwrap();
        println!("iter block {:?}", block);
        let mut block_iter = block.into_iter();
        while block_iter.valid() {
            block_iter.next();
            println!("{:?}", block_iter.current())
        }
    }

    fn table_opts_mid() -> Arc<TableOptions> {
        Arc::new(TableOptions {
            block_size: 11 * 20,
            table_size: 2 << 20,
            table_capacity: ((2_u64 << 20_u64) as f64 / 0.9) as usize,
            checksum: true,
            policy: BloomFilterPolicy::new(10),
        })
    }

    #[test]
    fn basic_test_iter_data() {
        let mut file = tempfile().unwrap();
        //let mut dst = unsafe { MmapMut::map_mut(&file).unwrap() };
        let data = vec![
            "aba", "abb", "abc", "abd", "abe", "abf", "abg", "abh", "abi", "abj", "abk", "abl",
            "abm", "abn", "abo", "abp", "abq", "abr", "abs", "abt", "abu", "abv", "abw", "abx",
            "aby", "abz",
        ];

        let opts = table_opts_mid();
        let mut table = TableBuilder::new_with_options(&mut file, opts.clone());

        for item in data {
            table.add(item.as_bytes(), item.as_bytes()).unwrap();
        }

        table.finish().unwrap();
        //file.sync_all().unwrap();

        let data = unsafe { Mmap::map(&file).unwrap() };
        let table = Table::open(12, data, opts.clone());
        let mut table_iter = table.iter().unwrap();

        // table_iter.next();
        // table_iter.seek(&[97, 98, 103]);

        while table_iter.valid() {
            println!("table_iter.current {:?}", table_iter.current());
            table_iter.next();
        }
        println!("---------------------------------------------------------------");
        table_iter.seek(&[97, 98, 103]);
        while table_iter.valid() {
            println!("table_iter.current {:?}", table_iter.current());
            table_iter.next();
        }
        println!("---------------------------------------------------------------");
        table_iter.seek(&[97, 98, 103]);
        while table_iter.valid() {
            println!("table_iter.current {:?}", table_iter.current());
            table_iter.prev();
        }
    }
}
