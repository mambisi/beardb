use crate::codec::decode_fixed32;
use std::collections::VecDeque;
use std::ops::Mul;

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

pub(crate) struct BloomFilterPolicy {
    bit_per_key: usize,
    k: usize,
}

fn bloom_hash(data: &[u8]) -> u32 {
    hash(data, 0xbc9f1d34)
}

impl BloomFilterPolicy {
    pub(crate) fn new(bit_per_key: usize) -> Self {
        let k = (bit_per_key as f64).mul(0.69) as usize;
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

    struct BloomTest {
        policy: BloomFilterPolicy,
        filter: Vec<u8>,
        keys: Vec<Vec<u8>>,
    }

    impl Default for BloomTest {
        fn default() -> Self {
            Self {
                policy: BloomFilterPolicy::new(10),
                filter: vec![],
                keys: vec![],
            }
        }
    }

    impl BloomTest {
        fn reset(&mut self) {
            self.keys.clear();
            self.filter.clear()
        }

        fn add(&mut self, key: &[u8]) {
            self.keys.push(key.to_vec())
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

        fn dump_filter(&self) {
            eprint!("F(");
            for c in self.filter.iter() {
                for j in 0..8 {
                    if ((*c as i32) & (1 << j)) != 0 {
                        eprint!("{}", '1')
                    } else {
                        eprint!("{}", '.')
                    }
                }
            }
            eprint!(")\n");
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

        fn false_positive_rate(&mut self) -> f64 {
            let mut results = 0.0;
            for i in 0..10000_i32 {
                if self.matches(&(i + 1000000000).to_le_bytes()) {
                    results += 1.0;
                }
            }
            return results / 10000.0;
        }
    }

    fn next_length(mut len: usize) -> usize {
        if len < 10 {
            len += 1;
        } else if len < 100 {
            len += 10;
        } else if len < 1000 {
            len += 100;
        } else {
            len += 1000;
        }
        return len;
    }

    #[test]
    fn empty_filter() {
        let mut t = BloomTest::default();
        assert!(!t.matches(b"hello"));
        assert!(!t.matches(b"world"));
    }

    #[test]
    fn small() {
        let mut t = BloomTest::default();
        t.add(b"hello");
        t.add(b"world");
        assert!(t.matches(b"hello"));
        assert!(t.matches(b"world"));
        assert!(!t.matches(b"x"));
        assert!(!t.matches(b"foo"));
    }

    #[test]
    fn varying_lengths() {
        let mut t = BloomTest::default();
        let mut mediocre_filters = 0;
        let mut good_filters = 0;
        let mut len = 1;
        while len <= 10000 {
            t.reset();
            for i in 0..len as i32 {
                t.add(&(i.to_le_bytes()))
            }
            t.build();
            assert!(
                t.filter_size() < (len * 10 / 8) + 40,"{}", len
            );
            for i in 0..len as i32 {
                assert!(
                    t.matches(&(i.to_le_bytes())),"Length {}; key {}", len, i
                )
            }

            let rate = t.false_positive_rate();
            eprintln!(
                "False positives: {} @ length = {} ; bytes = {}",
                rate * 100.0,
                len,
                t.filter_size()
            );
            assert!(rate < 0.02);
            if rate > 0.0125 {
                mediocre_filters += 1;
            } else {
                good_filters += 1;
            }
            len = next_length(len);
        }
        assert!(mediocre_filters < good_filters / 5)
    }
}
