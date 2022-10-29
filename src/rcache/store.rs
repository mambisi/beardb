use crate::rcache::policy::Policy;
use crate::rcache::{Entry, ItemCallBackFn};
use std::fmt::Debug;
use std::rc::Rc;
use std::sync::Arc;
use std::time::SystemTime;

pub(crate) trait Store<V>
where
    V: Clone,
{
    fn get(&self, key: u64, conflict: u64) -> Option<V>;
    fn set(&self, entry: Entry<V>);
    fn expiration(&self, key: u64) -> SystemTime;
    fn remove(&self, key: u64, conflict: u64) -> Option<(u64, V)>;
    fn update(&self, entry: Entry<V>) -> Option<V>;
    fn cleanup(&self, policy: Arc<dyn Policy>);
    fn clear(&self, callback: ItemCallBackFn<V>);
}
