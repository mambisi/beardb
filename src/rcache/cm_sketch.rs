use rand::RngCore;
use std::num::{NonZeroI64, NonZeroU64};
use arrayvec::ArrayVec;

const CM_DEPTH: usize = 4;
type CMRow = Box<[u8]>;

fn new_cm_row(num_counters: u64) -> CMRow {
    vec![0; num_counters as usize / 2].into_boxed_slice()
}

#[derive(Debug)]
pub(crate) struct CMSketch {
    rows: ArrayVec<CMRow, CM_DEPTH>,
    seed: ArrayVec<u64, CM_DEPTH>,
    mask: u64,
}

impl CMSketch {
    pub(crate) fn new(num_counters: u64) -> Self {
        let num_counters = next2power(num_counters as i64) as u64;
        let mut sketch = Self {
            rows: ArrayVec::new_const(),
            seed: ArrayVec::new_const(),
            mask: num_counters - 1,
        };
        let mut rand = rand::thread_rng();
        for (row, seed) in sketch.rows.iter_mut().zip(sketch.seed.iter_mut()) {
            *row = new_cm_row(num_counters);
            *seed = rand.next_u64();
        }
        sketch
    }

    pub(crate) fn increment(&mut self, hashed: u64) {
        for (row, seed) in self.rows.iter_mut().zip(self.seed.iter_mut()) {
            row.increment((hashed ^ *seed) & self.mask)
        }
    }

    pub(crate) fn estimate(&self, hashed: &u64) -> i64 {
        let mut min = 255_u8;
        for (row, seed) in self.rows.iter().zip(self.seed.iter()) {
            let val = row.get((hashed ^ *seed) & self.mask);
            if val < min {
                min = val
            }
        }
        i64::from(min)
    }

    pub(crate) fn reset(&mut self) {
        for r in self.rows.iter_mut() {
            r.reset()
        }
    }
    pub(crate) fn clear(&mut self) {
        for r in self.rows.iter_mut() {
            r.clear()
        }
    }
}
trait Row {
    fn get(&self, n: u64) -> u8;
    fn increment(&mut self, n: u64);
    fn reset(&mut self);
    fn clear(&mut self);
}

impl Row for CMRow {
    fn get(&self, n: u64) -> u8 {
        (self[n as usize / 2] >> ((n & 1) * 4)) & 0x0f
    }

    fn increment(&mut self, n: u64) {
        // Index of the counter.
        let i = n / 2;
        // Shift distance (even 0, odd 4).
        let s = (n & 1) * 4;
        // Counter value.
        let v = (self[i as usize] >> s) & 0x0f;
        // Only increment if not max value (overflow wrap is bad for LFU).
        if v < 15 {
            self[i as usize] += 1 << s;
        }
    }

    fn reset(&mut self) {
        for i in self.iter_mut() {
            *i = (*i >> 1) & 0x77;
        }
    }

    fn clear(&mut self) {
        for i in self.iter_mut() {
            *i = 0;
        }
    }
}

fn next2power(mut x: i64) -> i64 {
    x -= 1;
    x |= x >> 1;
    x |= x >> 2;
    x |= x >> 4;
    x |= x >> 8;
    x |= x >> 16;
    x |= x >> 32;
    x += 1;
    x
}