pub trait Codec {
    type ArchivedType;
    fn encode(&self) -> crate::Result<Vec<u8>>;
    fn decode(buf: &[u8]) -> crate::Result<&Self::ArchivedType>;
}
