use crate::codec::Codec;
use crate::Error;
use bytecheck::CheckBytes;
use rkyv::ser::serializers::AllocSerializer;
use rkyv::ser::Serializer;
use rkyv::{Archive, Deserialize, Serialize};
use std::cmp::Ordering;

#[derive(Archive, Deserialize, Serialize, Debug, PartialEq, Eq, PartialOrd, Ord)]
#[archive(compare(PartialEq))]
#[archive_attr(derive(CheckBytes, Debug))]
pub enum ValueType {
    Deletion = 0,
    Value = 1,
}

impl Into<u8> for ValueType {
    fn into(self) -> u8 {
        match self {
            ValueType::Deletion => 0,
            ValueType::Value => 1,
        }
    }
}

#[derive(Archive, Deserialize, Serialize, Debug, PartialEq, Eq)]
#[archive(compare(PartialEq))]
#[archive_attr(derive(CheckBytes, Debug))]
pub struct MemEntry {
    pub(crate) key: Vec<u8>,
    pub(crate) tag: u8,
    pub(crate) value: Vec<u8>,
}
impl PartialOrd for MemEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for MemEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.key.cmp(&other.key) {
            Ordering::Less => Ordering::Less,
            Ordering::Equal => self.tag.cmp(&other.tag),
            Ordering::Greater => Ordering::Greater,
        }
    }
}

impl MemEntry {
    pub fn new(tag: ValueType, key: Vec<u8>, value: Vec<u8>) -> Self {
        Self {
            key,
            tag: tag.into(),
            value,
        }
    }
    pub fn key(&self) -> &[u8] {
        &self.key
    }

    pub fn value(&self) -> &[u8] {
        &self.value
    }
}

impl Codec for MemEntry {
    type ArchivedType = ArchivedMemEntry;

    fn encode(&self) -> crate::Result<Vec<u8>> {
        let bytes = rkyv::to_bytes::<_, 256>(self).unwrap();
        Ok(bytes.to_vec())
    }

    fn decode(buf: &[u8]) -> crate::Result<&Self::ArchivedType> {
        rkyv::check_archived_root::<MemEntry>(buf).map_err(|err| Error::CodecError)
    }
}

#[cfg(test)]
mod tests {
    use crate::codec::Codec;
    use crate::types::{MemEntry, ValueType};

    #[test]
    fn test_comparator() {
        let a = MemEntry::new(ValueType::Value, vec![4, 4, 4], vec![5, 5, 5]);
        println!("{:?}", a.encode().unwrap())
    }
}
