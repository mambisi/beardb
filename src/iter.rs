use crate::Result;

pub trait Iter {
    type Item;
    fn valid(&self) -> bool;
    fn prev(&mut self);
    fn next(&mut self);
    fn current(&self) -> Option<Self::Item>;
    fn seek(&mut self, target: &[u8]);
    fn seek_to_first(&mut self);
    fn seek_to_last(&mut self);
}
