use crate::Result;

pub trait Iter {
    type Item;
    fn valid(&self) -> bool;
    fn prev(&mut self) -> Result<()>;
    fn next(&mut self) -> Result<()>;
    fn current(&self) -> Result<Option<Self::Item>>;
    fn seek(&mut self, target: &[u8]) -> Result<()>;
    fn seek_to_first(&mut self);
    fn seek_to_last(&mut self);
}
