use std::time::SystemTime;

use crate::{Entry, ItemCallBackFn};
use crate::policy::Policy;

pub(crate) trait Store<V>
    where
        V: Clone,
{
    fn get(&self, key: u64, conflict: u64) -> Option<V>;
    fn set(&self, entry: Entry<V>);
    fn expiration(&self, key: u64) -> SystemTime;
    fn remove(&self, key: u64, conflict: u64) -> Option<(u64, V)>;
    fn update(&self, entry: &Entry<V>) -> Option<V>;
    fn cleanup(&self, policy: &dyn Policy);
    fn clear(&self, callback: ItemCallBackFn<V>);
}
