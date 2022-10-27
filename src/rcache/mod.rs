use crate::rcache::store::CachedItem;
use chrono::{DateTime, Utc};
use std::fmt::Debug;
use std::ops::Add;
use std::rc::Rc;
use std::sync::Arc;
use std::time::{Duration, Instant};

mod bloom;
mod cm_sketch;
mod policy;
mod ring;
mod sharded_map;
mod store;
mod ttl;
mod utils;

#[derive(Clone, Debug)]
pub(crate) enum EntryFlag {
    New,
    Delete,
    Update,
}

#[derive(Clone, Debug)]
pub(crate) struct Entry<V>
where
    V: CachedItem,
{
    flag: EntryFlag,
    key: u64,
    conflict: u64,
    value: V,
    cost: i64,
    exp: DateTime<Utc>,
}
type ItemCallBackFn<V> = Arc<Option<Box<dyn 'static + Send + Sync + ItemCallback<V>>>>;
pub(crate) trait ItemCallback<V>: Fn(&Entry<V>) {}

impl<F, V> ItemCallback<V> for F where F: Fn(&Entry<V>) {}

pub(crate) struct Config<V>
where
    V: CachedItem,
{
    on_evict: ItemCallBackFn<V>,
    on_reject: ItemCallBackFn<V>,
}
