use crate::codec::{Codec};
use crate::{ensure, Error};
use std::io::{Read};
use std::mem::size_of;

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub enum ValueType {
    Deletion = 0,
    Value = 1,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct MemEntry<'a> {
    tag: u64,
    key: &'a [u8],
    value: &'a [u8],
}



impl<'a> MemEntry<'a> {
    pub(crate) fn new(seq: u64, value_type: ValueType, key: &'a [u8], value: &'a [u8]) -> Self {
        let seq = seq << 8 | value_type as u64;
        Self { key, value, tag: seq }
    }

    pub(crate) fn value_type(&self) -> ValueType {
        let typ = self.tag & 0xff;
        match typ {
            0 => ValueType::Deletion,
            1 => ValueType::Value,
            _ => ValueType::Value,
        }
    }

    pub(crate) fn seq(&self) -> u64 {
        self.tag >> 8
    }

    pub(crate) fn key(&self) -> &'a[u8] {
        self.key
    }

    pub(crate) fn value(&self) -> &'a[u8] {
        self.value
    }
}

impl<'a> Codec<'a> for MemEntry<'a> {

    fn encode(&self) -> crate::Result<Vec<u8>> {
        ensure!(!self.key.is_empty(), Error::CodecError);
        let key_size = (self.key.len() + size_of::<u64>()) as u32;
        let value_size = self.value.len() as u32;
        let mut bytes = Vec::with_capacity((key_size + value_size) as usize);
        bytes.extend_from_slice(&key_size.to_le_bytes());
        bytes.extend_from_slice(&self.key);
        bytes.extend_from_slice(&self.tag.to_le_bytes());
        bytes.extend_from_slice(&value_size.to_le_bytes());
        bytes.extend_from_slice(&self.value);
        Ok(bytes)
    }

    fn decode_from_slice(buf: &'a[u8]) -> crate::Result<Self>{
        let mut cursor = 0_usize;
        let mut buffer_len = buf.len();
        let key_offset = size_of::<u32>();
        cursor += key_offset;
        ensure!(cursor <= buffer_len, Error::CodecError);
        let key_size  = unsafe { (buf[..cursor].as_ptr() as *const u32).read_unaligned() } as usize;
        let tag_offset = size_of::<u64>();
        cursor += key_size;
        ensure!(cursor <= buffer_len, Error::CodecError);
        let key  = &buf[key_offset..cursor - tag_offset];
        let tag =  unsafe { (buf[key_offset +  key_size - tag_offset..cursor].as_ptr() as *const u64).read_unaligned() };
        let value_offset = size_of::<u32>();
        cursor += value_offset;
        ensure!(cursor <= buffer_len, Error::CodecError);
        let value_size  = unsafe { (buf[key_offset +  key_size..cursor].as_ptr() as *const u32).read_unaligned() } as usize;
        cursor += value_size;
        ensure!(cursor <= buffer_len, Error::CodecError);
        let value  = &buf[key_offset +  key_size + value_offset..cursor];
        Ok(Self {
            tag,
            key,
            value
        })
    }

    fn decode_from_reader<R: Read>(_: R) -> crate::Result<Self> {
        unimplemented!()
    }
}

#[cfg(test)]
mod tests {
    use crate::codec::Codec;
    use crate::types::{MemEntry, ValueType};

    #[test]
    fn test_codec() {
        let key  = [3; 24].as_slice();
        let value  = [8; 12].as_slice();
        let entry = MemEntry::new(1, ValueType::Value, key, value);
        let encoded = entry.encode().unwrap();
        let dentry = MemEntry::decode_from_slice(&encoded).unwrap();
        assert_eq!(entry, dentry);
        assert_eq!(entry.seq(), dentry.seq());
        assert_eq!(entry.value_type(), dentry.value_type());
    }
}
