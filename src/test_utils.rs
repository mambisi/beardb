pub(crate) trait BloomTest {
    fn reset(&mut self);

    fn add(&mut self, key: &[u8]);

    fn matches(&mut self, key: &[u8]) -> bool;

    fn filter_size(&self) -> usize;

    fn false_positive_rate(&mut self) -> f64 {
        let mut results = 0.0;
        for i in 0..10000_i32 {
            if self.matches(&(i + 1000000000).to_le_bytes()) {
                results += 1.0;
            }
        }
        return results / 10000.0;
    }

    fn build(&mut self){

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

pub(crate) fn empty_filter<T>(mut t: T) where T : BloomTest {
    assert!(!t.matches(b"hello"));
    assert!(!t.matches(b"world"));
}

pub(crate) fn small<T>(mut t: T) where T : BloomTest {
    t.add(b"hello");
    t.add(b"world");
    assert!(t.matches(b"hello"));
    assert!(t.matches(b"world"));
    assert!(!t.matches(b"x"));
    assert!(!t.matches(b"foo"));
}

pub(crate) fn varying_lengths<T>(mut t: T) where T : BloomTest {
    let mut mediocre_filters = 0;
    let mut good_filters = 0;
    let mut len = 1;
    while len <= 10000 {
        t.reset();
        for i in 0..len as i32 {
            t.add(&(i.to_le_bytes()))
        }
        t.build();
        assert!(t.filter_size() < (len * 10 / 8) + 40, "{}", len);
        for i in 0..len as i32 {
            assert!(t.matches(&(i.to_le_bytes())), "Length {}; key {}", len, i)
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