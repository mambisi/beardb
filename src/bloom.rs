use crate::codec::decode_fixed32;
use std::io::Write;

fn hash(data: &[u8], seed: u32) -> u32 {
    let m: u32 = 0xc6a4a793;
    let r: u32 = 24;

    let mut ix = 0;
    let limit = data.len();

    let mut h: u32 = seed ^ (limit as u64 * m as u64) as u32;

    while ix + 4 <= limit {
        let w: u32 = decode_fixed32(&data[ix..ix + 4]);
        ix += 4;

        h = (h as u64 + w as u64) as u32;
        h = (h as u64 * m as u64) as u32;
        h ^= h >> 16;
    }
    assert!(limit - ix < 4);
    if limit - ix > 0 {
        let mut i = 0;

        for b in data[ix..].iter() {
            h = h.overflowing_add((*b as u32) << (8 * i)).0;
            i += 1;
        }

        h = (h as u64 * m as u64) as u32;
        h ^= h >> r;
    }
    h
}

#[derive(Debug)]
pub(crate) struct BloomFilterPolicy {
    bit_per_key: usize,
    k: usize,
}

pub(crate) fn bloom_hash(data: &[u8]) -> u32 {
    hash(data, 0xbc9f1d34)
}

impl BloomFilterPolicy {
    pub(crate) fn new(bit_per_key: usize) -> Self {
        let k = ((bit_per_key as f64) * 0.69) as usize;
        Self { bit_per_key, k }
    }

    pub(crate) fn create_filter<'a, Keys: AsRef<[&'a [u8]]>>(&self, keys: Keys, dst: &mut Vec<u8>) {
        let mut bits = keys.as_ref().len() * self.bit_per_key;
        if bits < 64 {
            bits = 64;
        }
        let bytes = (bits + 7) / 8;
        bits = bytes * 8;
        let initial_size = dst.len();
        dst.resize(bytes + initial_size, 0);
        dst.push(self.k as u8);

        let array = &mut dst[initial_size..];
        for key in keys.as_ref() {
            // Use double-hashing to generate a sequence of hash values.
            // See analysis in [Kirsch,Mitzenmacher 2006].
            let mut h = bloom_hash(key);
            let delta = (h >> 17) | (h << 15);

            for _ in 0..self.k {
                let bitpos = h % (bits as u32);
                array[(bitpos / 8) as usize] |= 1 << (bitpos % 8);
                h = h.overflowing_add(delta).0;
            }
        }
    }

    pub(crate) fn create_filter_from_hashes(&self, hashes: &[u32]) -> Vec<u8> {
        let mut bits = hashes.as_ref().len() * self.bit_per_key;
        if bits < 64 {
            bits = 64;
        }
        let bytes = (bits + 7) / 8;
        bits = bytes * 8;
        let mut dst = vec![0; bytes];
        dst.push(self.k as u8);

        let array = &mut dst[0..];
        for key_hash in hashes {
            // Use double-hashing to generate a sequence of hash values.
            // See analysis in [Kirsch,Mitzenmacher 2006].
            let mut h = *key_hash;
            let delta = (h >> 17) | (h << 15);
            for _ in 0..self.k {
                let bitpos = h % (bits as u32);
                array[(bitpos / 8) as usize] |= 1 << (bitpos % 8);
                h = h.overflowing_add(delta).0;
            }
        }
        dst
    }

    pub(crate) fn key_and_match(&self, key: &[u8], bloom_filter: &[u8]) -> bool {
        if bloom_filter.len() < 2 {
            return false;
        }
        let bits = (bloom_filter.len() - 1) * 8;
        let k = bloom_filter[bloom_filter.len() - 1] as usize;
        if k > 30 {
            return true;
        }

        let mut h = bloom_hash(key);
        let delta = (h >> 17) | (h << 15);
        for _ in 0..k {
            let bitpos = h % (bits as u32);
            if (bloom_filter[(bitpos / 8) as usize] & (1 << (bitpos % 8))) == 0 {
                return false;
            }
            h = h.overflowing_add(delta).0;
        }
        return true;
    }
}

#[cfg(test)]
mod test {
    use crate::bloom::BloomFilterPolicy;
    use crate::test_utils;
    use crate::test_utils::BloomTest;

    struct BloomTestImpl {
        policy: BloomFilterPolicy,
        filter: Vec<u8>,
        keys: Vec<Vec<u8>>,
    }

    impl Default for BloomTestImpl {
        fn default() -> Self {
            Self {
                policy: BloomFilterPolicy::new(10),
                filter: vec![],
                keys: vec![],
            }
        }
    }

    impl BloomTest for BloomTestImpl {
        fn reset(&mut self) {
            self.keys.clear();
            self.filter.clear()
        }

        fn add(&mut self, key: &[u8]) {
            self.keys.push(key.to_vec())
        }

        fn matches(&mut self, key: &[u8]) -> bool {
            if !self.keys.is_empty() {
                self.build();
            }
            self.policy.key_and_match(key, &mut self.filter)
        }

        fn filter_size(&self) -> usize {
            self.filter.len()
        }

        fn build(&mut self) {
            let mut key_slices = Vec::new();
            for key in self.keys.iter() {
                key_slices.push(key.as_slice())
            }
            self.filter.clear();
            self.policy.create_filter(key_slices, &mut self.filter);
            self.keys.clear()
        }
    }

    #[test]
    fn empty_filter() {
        let mut t = BloomTestImpl::default();
        test_utils::empty_filter(t)
    }

    #[test]
    fn small() {
        let mut t = BloomTestImpl::default();
        test_utils::small(t)
    }

    #[test]
    fn varying_lengths() {
        let mut t = BloomTestImpl::default();
        test_utils::varying_lengths(t)
    }
}
