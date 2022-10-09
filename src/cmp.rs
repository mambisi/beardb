use crate::types::MemEntry;
use crate::Error;
use std::cmp::Ordering;
use crate::codec::Codec;

pub trait Comparator {
    fn cmp(&self, a: &[u8], b: &[u8]) -> crate::Result<Ordering>;
}

pub struct MemTableComparator;

impl Comparator for MemTableComparator {
    fn cmp(&self, a: &[u8], b: &[u8]) -> crate::Result<Ordering> {
        let a = MemEntry::decode(a)?;
        let b = MemEntry::decode(b)?;

        let ord = match a.key.cmp(&b.key) {
            Ordering::Less => {
                Ordering::Less
            }
            Ordering::Equal => {
                a.tag.cmp(&b.tag)
            }
            Ordering::Greater => {
                Ordering::Greater
            }
        };
        Ok(ord)
    }
}


pub struct DefaultComparator;

impl Comparator for DefaultComparator {
    fn cmp(&self, a: &[u8], b: &[u8]) -> crate::Result<Ordering> {
        Ok(a.cmp(b))
    }
}