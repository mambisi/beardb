use crate::block::Block;
use crate::bloom::BloomFilterPolicy;
use crate::codec::decode_fixed32;
use crate::constant::BLOCK_ENTRY_HEADER_SIZE;
use crate::table_index::{BlockIndex, TableIndexReader};
use crate::{codec, Error};
use core::slice::SlicePattern;
use memmap2::Mmap;
use std::sync::Arc;

pub(crate) fn decode_key(data: &[u8]) -> &[u8] {
    //TODO: Handle index errors
    let key_size = decode_fixed32(&data[..4]) as usize;
    println!("key_size {}", key_size);
    &data[BLOCK_ENTRY_HEADER_SIZE..BLOCK_ENTRY_HEADER_SIZE + key_size]
}

pub(crate) fn decode_key_value(data: &[u8]) -> (&[u8], &[u8]) {
    //TODO: Handle index errors
    let key_size = decode_fixed32(&data[..4]) as usize;
    let value_size = decode_fixed32(&data[4..8]) as usize;
    println!("key_size {} value_size {}", key_size, value_size);
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
    pub(crate) policy: BloomFilterPolicy,
}

impl Default for TableOptions {
    fn default() -> Self {
        Self {
            block_size: 4 * 1024,
            table_size: 2 << 20,
            table_capacity: ((2_u64 << 20_u64) as f64 / 0.9) as usize,
            policy: BloomFilterPolicy::new(10),
        }
    }
}

pub(crate) struct Table {
    file: Mmap,
    id: u64,
    index_start: usize,
    index_len: usize,
    opts: Arc<TableOptions>,
}

impl Table {
    fn open(id: u64, file: Mmap, opts: Arc<TableOptions>) -> Table {
        let s = &file[file.as_slice().len() - 8..file.as_slice().len() - 4];
        let index_len = codec::decode_fixed32(s) as usize;
        let index_start = file.as_slice().len() - index_len - 8;

        Table {
            file,
            id,
            index_start,
            index_len,
            opts,
        }
    }

    fn index(&self) -> TableIndexReader {
        TableIndexReader::open(
            &self.file[self.index_start..(self.index_start + self.index_len)],
            self.opts.clone(),
        )
    }

    fn get(&self, key: &[u8]) -> Option<&[u8]> {
        let index = self.index();
        let block_index = match index.find_key_block(key) {
            None => return None,
            Some(index) => index,
        };
        let raw_block = &self.file[block_index.offset_start..block_index.offset_end];
        let block = Block::open(block_index, raw_block);
        if let Some((start, end)) = block.get_value_offset_abs(key) {
            return Some(&self.file[start..end]);
        }
        return None;
    }
}

#[cfg(test)]
mod test {
    use crate::bloom::BloomFilterPolicy;
    use crate::table::{Table, TableOptions};
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
        let table = Table::open(12, data, opts.clone());
        let index = table.index();
        println!("block index {:?}", index.find_key_block(&[97, 98, 113]));
        println!("---------------------------------------------------------------");
        println!("block index {:?}", index.find_key_block(b"abr"));
        println!("Value {:?}", table.get(b"abr"));
        println!("---------------------------------------------------------------");
        println!("block index {:?}", index.find_key_block(b"aba"));
        println!("Value {:?}", table.get(b"aba"));
        println!("---------------------------------------------------------------");
        println!("block index {:?}", index.find_key_block(b"zzz"));
    }
}
