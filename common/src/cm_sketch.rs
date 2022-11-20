use arrayvec::ArrayVec;
use rand::RngCore;

const CM_DEPTH: usize = 4;

#[derive(Debug, Clone)]
struct CMRow(Box<[u8]>);

fn new_cm_row(num_counters: u64) -> CMRow {
    CMRow(vec![0; num_counters as usize / 2].into_boxed_slice())
}

#[derive(Debug, Clone)]
pub struct CMSketch {
    rows: ArrayVec<CMRow, CM_DEPTH>,
    seed: ArrayVec<u64, CM_DEPTH>,
    mask: u64,
}

impl CMSketch {
    pub fn new(num_counters: u64) -> Self {
        let num_counters = next2power(num_counters as i64) as u64;
        let mut sketch = Self {
            rows: ArrayVec::new_const(),
            seed: ArrayVec::new_const(),
            mask: num_counters - 1,
        };
        let mut rand = rand::thread_rng();
        for _ in 0..CM_DEPTH {
            sketch.rows.push(new_cm_row(num_counters));
            sketch.seed.push(rand.next_u64());
        }
        sketch
    }

    pub fn increment(&mut self, hashed: u64) {
        for (row, seed) in self.rows.iter_mut().zip(self.seed.iter_mut()) {
            row.increment((hashed ^ *seed) & self.mask)
        }
    }

    pub fn estimate(&self, hashed: &u64) -> i64 {
        let mut min = 255_u8;
        for (row, seed) in self.rows.iter().zip(self.seed.iter()) {
            let val = row.get((hashed ^ *seed) & self.mask);
            if val < min {
                min = val
            }
        }
        i64::from(min)
    }

    pub fn reset(&mut self) {
        for r in self.rows.iter_mut() {
            r.reset()
        }
    }
    pub fn clear(&mut self) {
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
        (self.0[n as usize / 2] >> ((n & 1) * 4)) & 0x0f
    }

    fn increment(&mut self, n: u64) {
        // Index of the counter.
        let i = n / 2;
        // Shift distance (even 0, odd 4).
        let s = (n & 1) * 4;
        // Counter value.
        let v = (self.0[i as usize] >> s) & 0x0f;
        // Only increment if not max value (overflow wrap is bad for LFU).
        if v < 15 {
            self.0[i as usize] += 1 << s;
        }
    }

    fn reset(&mut self) {
        for i in self.0.iter_mut() {
            *i = (*i >> 1) & 0x77;
        }
    }

    fn clear(&mut self) {
        for i in self.0.iter_mut() {
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

#[cfg(test)]
mod test {
    use crate::cm_sketch::CMSketch;

    #[test]
    fn test_increment() {
        let mut sketch = CMSketch::new(16);
        sketch.increment(1);
        sketch.increment(1);
        assert_eq!(2, sketch.estimate(&1));
        assert_eq!(0, sketch.estimate(&0));
    }
}