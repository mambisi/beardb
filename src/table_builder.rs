use crate::block::BlockBuilder;
use crate::constant::{BLOCK_ENTRY_HEADER_SIZE, BLOCK_META_SIZE, CHECKSUM_SIZE};
use crate::table::TableOptions;
use crate::table_index::TableIndexBuilder;
use crate::{bloom};
use core::slice::SlicePattern;
use std::io::Write;
use std::sync::Arc;

pub(crate) struct TableBuilder<W> {
    dst: W,
    opt: Arc<TableOptions>,
    current: BlockBuilder,
    blocks: Vec<u8>,
    filters: Vec<Vec<u8>>,
    index: Vec<u32>,
    entries_offset: u32,
    blocks_offset: u32,
    key_count: u32,
}

impl<W> TableBuilder<W>
where
    W: Write,
{
    pub(crate) fn new(dst: W) -> TableBuilder<W> {
        let opt = Default::default();
        TableBuilder::new_with_options(dst, opt)
    }

    pub(crate) fn new_with_options(dst: W, opt: Arc<TableOptions>) -> TableBuilder<W> {
        Self {
            dst,
            opt,
            current: Default::default(),
            blocks: vec![],
            index: vec![],
            entries_offset: 0,
            blocks_offset: 0,
            filters: vec![],
            key_count: 0,
        }
    }

    pub(crate) fn add<'a>(&mut self, key: &'a [u8], value: &'a [u8]) -> crate::Result<()> {
        if self.should_finish_block(key, value) {
            self.index.push(self.blocks_offset);
            self.filters.push(
                self.opt
                    .policy
                    .create_filter_from_hashes(self.current.key_hashes.as_slice()),
            );
            self.blocks_offset += self.current.finish(&mut self.blocks)? as u32;
        }
        self.add_internal(key, value)
    }

    fn add_internal<'a>(&mut self, key: &'a [u8], value: &'a [u8]) -> crate::Result<()> {
        let block = &mut self.current;
        let mut written_bytes = 0_usize;
        block.entry_offsets.push(block.entries_offset);
        written_bytes += block
            .data
            .write((key.len() as u32).to_le_bytes().as_slice())?;
        written_bytes += block
            .data
            .write((value.len() as u32).to_le_bytes().as_slice())?;
        written_bytes += block.data.write(key)?;
        written_bytes += block.data.write(value)?;
        block.key_hashes.push(bloom::bloom_hash(key));
        block.entry_count += 1;
        block.entries_offset += written_bytes as u32;
        self.key_count += 1;
        Ok(())
    }

    fn should_finish_block<'a>(&mut self, key: &'a [u8], value: &'a [u8]) -> bool {
        let block = &self.current;
        if block.data.is_empty() {
            return false;
        }
        let est_block_size = block.data.len()
            + BLOCK_ENTRY_HEADER_SIZE
            + key.len()
            + value.len()
            + (block.entry_offsets.len() * 4)
            + BLOCK_META_SIZE
            + CHECKSUM_SIZE;
        est_block_size > self.opt.block_size
    }

    pub(crate) fn finish(mut self) -> crate::Result<()> {
        if !self.current.data.is_empty() {
            self.index.push(self.blocks_offset);
            self.filters.push(
                self.opt
                    .policy
                    .create_filter_from_hashes(self.current.key_hashes.as_slice()),
            );
            let _ = self.current.finish(&mut self.blocks)?;
        }
        let index = TableIndexBuilder::new(&self.blocks, &self.index, self.filters, self.key_count);
        let index_block = index.finish()?;
        let index_block_size = index_block.len() as u32;
        let crc = crc32fast::hash(&index_block);
        self.dst.write_all(self.blocks.as_slice())?;
        self.dst.write_all(&index_block)?;
        self.dst
            .write_all(index_block_size.to_le_bytes().as_slice())?;
        self.dst.write_all(crc.to_le_bytes().as_slice())?;
        self.dst.flush()?;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use crate::bloom::BloomFilterPolicy;
    use crate::table::TableOptions;
    use crate::table_builder::TableBuilder;
    use std::sync::Arc;

    fn table_opts() -> Arc<TableOptions> {
        Arc::new(TableOptions {
            block_size: 11 * 5,
            table_size: 2 << 20,
            table_capacity: ((2_u64 << 20_u64) as f64 / 0.9) as usize,
            checksum: false,
            policy: BloomFilterPolicy::new(10),
        })
    }
    #[test]
    fn basic_test() {
        let mut dst = Vec::new();
        let data = vec!["aba", "abb", "abc"];

        let opts = table_opts();
        let mut table = TableBuilder::new_with_options(&mut dst, opts);

        for item in data {
            table.add(item.as_bytes(), item.as_bytes()).unwrap();
        }

        table.finish().unwrap();

        println!("{:?}", dst)
    }

    #[test]
    fn basic_test_more_data() {
        let mut dst = Vec::new();
        let data = vec![
            "aba", "abb", "abc", "abd", "abe", "abf", "abg", "abh", "abi", "abj", "abk", "abl",
            "abm", "abn", "abo", "abp", "abq", "abr", "abs", "abt", "abu", "abv", "abw", "abx",
            "aby", "abz",
        ];

        let opts = table_opts();
        let mut table = TableBuilder::new_with_options(&mut dst, opts);

        for item in data {
            table.add(item.as_bytes(), item.as_bytes()).unwrap();
        }

        table.finish().unwrap();
    }
}
