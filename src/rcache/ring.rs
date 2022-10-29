use crate::rcache::pool::Pool;
use std::sync::Arc;

pub(crate) trait Consumer {
    fn push(&self, keys: Vec<u64>) -> bool;
}

struct RingStripe {
    consumer: Arc<dyn Send + Sync + Consumer>,
    data: Vec<u64>,
    capacity: usize,
}

impl RingStripe {
    fn new(consumer: Arc<dyn Send + Sync + Consumer>, capacity: usize) -> Self {
        Self {
            consumer,
            data: Vec::with_capacity(capacity),
            capacity,
        }
    }

    fn push(&mut self, item: u64) {
        self.data.push(item);
        if self.data.len() >= self.capacity {
            if self.consumer.push(self.data.clone()) {
                self.data.clear()
            } else {
                self.data.truncate(0)
            }
        }
    }
}

pub(crate) struct RingBuffer {
    pool: Arc<Pool<RingStripe>>,
}

impl RingBuffer {
    pub(crate) fn new(
        consumer: Arc<dyn Send + Sync + Consumer>,
        pool_capacity: usize,
        capacity: usize,
    ) -> Self {
        Self {
            pool: Arc::new(Pool::new(pool_capacity, || {
                RingStripe::new(consumer.clone(), capacity)
            })),
        }
    }

    pub(crate) fn push(&self, item: u64) {
        if let Some(mut p) = self.pool.try_pull() {
            p.push(item)
        }
    }
}
