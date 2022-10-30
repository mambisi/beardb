#![warn(non_camel_case_types, non_upper_case_globals, unused_qualifications)]
#![allow(clippy::unreadable_literal, clippy::bool_comparison)]

use std::cmp;

use bit_vec::BitVec;

/// Bloom filter structure
#[derive(Clone, Debug)]
pub struct Bloom {
    bit_vec: BitVec,
    bitmap_bits: u64,
    k_num: u32,
}

impl Bloom {
    pub fn new_with_seed(bitmap_size: usize, items_count: usize) -> Self {
        assert!(bitmap_size > 0 && items_count > 0);
        let bitmap_bits = (bitmap_size as u64) * 8u64;
        let k_num = Self::optimal_k_num(bitmap_bits, items_count);
        let bitmap = BitVec::from_elem(bitmap_bits as usize, false);
        Self {
            bit_vec: bitmap,
            bitmap_bits,
            k_num,
        }
    }

    pub fn new(items_count: usize, fp_p: f64) -> Self {
        let bitmap_size = Self::compute_bitmap_size(items_count, fp_p);
        Bloom::new_with_seed(bitmap_size, items_count)
    }

    fn compute_bitmap_size(items_count: usize, fp_p: f64) -> usize {
        assert!(items_count > 0);
        if fp_p > 1.0 {
            return fp_p as usize;
        }
        let log2 = std::f64::consts::LN_2;
        let log2_2 = log2 * log2;
        ((items_count as f64) * f64::ln(fp_p) / (-8.0 * log2_2)).ceil() as usize
    }

    /// Record the presence of an item.
    pub fn set(&mut self, hash: u64) {
        for _ in 0..self.k_num {
            let bit_offset = (hash % self.bitmap_bits) as usize;
            self.bit_vec.set(bit_offset, true);
        }
    }

    /// Check if an item is present in the set.
    /// There can be false positives, but no false negatives.
    pub fn check(&self, hash: &u64) -> bool {
        for _ in 0..self.k_num {
            let bit_offset = (hash % self.bitmap_bits) as usize;
            if self.bit_vec.get(bit_offset).unwrap() == false {
                return false;
            }
        }
        true
    }

    /// Record the presence of an item in the set,
    /// and return the previous state of this item.
    pub fn check_and_set(&mut self, item: u64) -> bool {
        let mut found = true;
        for _ in 0..self.k_num {
            let bit_offset = (item % self.bitmap_bits) as usize;
            if self.bit_vec.get(bit_offset).unwrap() == false {
                found = false;
                self.bit_vec.set(bit_offset, true);
            }
        }
        found
    }

    /// Return the bitmap as a vector of bytes
    pub fn bitmap(&self) -> Vec<u8> {
        self.bit_vec.to_bytes()
    }

    /// Return the bitmap as a "BitVec" structure
    pub fn bit_vec(&self) -> &BitVec {
        &self.bit_vec
    }

    /// Return the number of bits in the filter
    pub fn number_of_bits(&self) -> u64 {
        self.bitmap_bits
    }

    /// Return the number of hash functions used for `check` and `set`
    pub fn number_of_hash_functions(&self) -> u32 {
        self.k_num
    }

    #[allow(dead_code)]
    fn optimal_k_num(bitmap_bits: u64, items_count: usize) -> u32 {
        let m = bitmap_bits as f64;
        let n = items_count as f64;
        let k_num = (m / n * f64::ln(2.0f64)).ceil() as u32;
        cmp::max(k_num, 1)
    }

    /// Clear all of the bits in the filter, removing all keys from the set
    pub fn clear(&mut self) {
        self.bit_vec.clear()
    }
}

#[cfg(test)]
mod test {
    use xxhash_rust::xxh3::xxh3_64;

    use test_utils::bloom_test::BloomTest;

    use crate::bloom::Bloom;

    const N: usize = 1 << 16;

    struct BloomTestImpl {
        bloom: Bloom,
        keys: Vec<Vec<u8>>,
    }

    impl Default for BloomTestImpl {
        fn default() -> Self {
            Self {
                bloom: Bloom::new(N * 10, 7.0),
                keys: vec![],
            }
        }
    }

    impl BloomTest for BloomTestImpl {
        fn reset(&mut self) {
            self.bloom.clear()
        }

        fn add(&mut self, key: &[u8]) {
            self.keys.push(key.to_vec())
        }

        fn matches(&mut self, key: &[u8]) -> bool {
            if !self.keys.is_empty() {
                self.build();
            }
            self.bloom.check_and_set(xxh3_64(key))
        }

        fn filter_size(&self) -> usize {
            self.bloom.bit_vec.len()
        }

        fn build(&mut self) {
            self.bloom.clear();
            for key in self.keys.iter() {
                self.bloom.set(xxh3_64(key.as_slice()))
            }
            self.keys.clear()
        }
    }

    #[test]
    fn empty_filter() {
        let t = BloomTestImpl::default();
        test_utils::bloom_test::empty_filter(t)
    }

    #[test]
    fn small() {
        let t = BloomTestImpl::default();
        test_utils::bloom_test::small(t)
    }
}
