use std::sync::Arc;

use crossbeam::channel::{Receiver, Sender};
use parking_lot::Mutex;

use crate::{Entry, PartialEntry};

#[derive(Copy, Clone, Debug)]
pub enum Event {
    Evict,
    Reject,
    Exit,
}

#[derive(Clone)]
struct Bus<T>(Sender<T>, Receiver<T>);

impl<T> Bus<T> {
    fn new(capacity: usize) -> Self {
        let (tx, rx) = crossbeam::channel::bounded(capacity);
        Bus(tx, rx)
    }
    fn add_rx(&self) -> Receiver<T> {
        self.1.clone()
    }
}

#[derive(Clone)]
pub(crate) struct Broadcast<V: Clone> {
    eviction_bus: Bus<PartialEntry<V>>,
    rejection_bus: Bus<PartialEntry<V>>,
    exit_bus: Bus<PartialEntry<V>>,
}

unsafe impl<V: Send + Sync + Clone> Send for Broadcast<V> {}

unsafe impl<V: Send + Sync + Clone> Sync for Broadcast<V> {}


impl<V: Clone> Broadcast<V> {
    pub(crate) fn new(buffer_size: usize) -> Self {
        Self {
            eviction_bus: Bus::new(buffer_size),
            rejection_bus: Bus::new(buffer_size),
            exit_bus: Bus::new(buffer_size),
        }
    }
    pub(crate) fn send(&self, event: Event, payload: PartialEntry<V>) {
        let bus = self.bus(event);
        println!("sending {:?}", event);
        bus.0.send(payload).unwrap();
        println!("sent {:?}", event);
    }

    fn bus(&self, event: Event) -> &Bus<PartialEntry<V>> {
        let bus = match event {
            Event::Evict => {
                &self.eviction_bus
            }
            Event::Reject => {
                &self.rejection_bus
            }
            Event::Exit => {
                &self.exit_bus
            }
        };
        bus
    }
    pub(crate) fn subscribe(&self, event: Event) -> Subscription<V> {
        let bus = self.bus(event);
        let rx = bus.add_rx();
        Subscription {
            rx
        }
    }
}


pub struct Subscription<V: Clone> {
    rx: Receiver<PartialEntry<V>>,
}

impl<V: Clone> AsRef<Receiver<PartialEntry<V>>> for Subscription<V> {
    fn as_ref(&self) -> &Receiver<PartialEntry<V>> {
        &self.rx
    }
}
