use std::time::SystemTime;

use crate::{Entry, Handler};
use crate::policy::Policy;

pub(crate) trait Store<V>
    where
        V: Clone,
{
    fn get(&self, key: u64, conflict: u64) -> Option<V>;
    fn set(&self, entry: Entry<V>);
    fn expiration(&self, key: u64) -> SystemTime;
    fn remove(&self, key: u64, conflict: u64) -> (u64, Option<V>);
    fn update(&self, entry: &Entry<V>) -> Option<V>;
    fn cleanup(&self, policy: &dyn Policy, handler: &dyn Handler<V>);
    fn clear(&self, callback: &dyn Handler<V>);
}
