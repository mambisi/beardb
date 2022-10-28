use crate::rcache::policy::Policy;
use crate::rcache::{Entry, ItemCallBackFn};
use chrono::{DateTime, Utc};
use std::fmt::Debug;
use std::rc::Rc;
use std::sync::Arc;

pub(crate) trait Store<V>
{
    fn get(&self, key: u64, conflict: u64) -> Option<Arc<V>>;
    fn set(&self, entry: Entry<V>);
    fn expiration(&self, key: u64) -> DateTime<Utc>;
    fn remove(&self, key: u64, conflict: u64) -> Option<(u64, Arc<V>)>;
    fn update(&self, entry: Entry<V>) -> Option<Arc<V>>;
    fn cleanup(&self, policy: Arc<dyn Policy>, on_evict: ItemCallBackFn<V>);
    fn clear(&self, callback: ItemCallBackFn<V>);
}
