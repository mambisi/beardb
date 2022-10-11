use std::io::Read;
use std::u64;

pub(crate) trait Codec<'de> : Sized {
    fn encode(&self) -> crate::Result<Vec<u8>>;
    fn decode_from_slice(buf: &'de[u8]) -> crate::Result<Self>;
    fn decode_from_reader<R: Read>(reader: R) -> crate::Result<Self>;
}

pub(crate) struct Reader<R> where R : Read{
    rdr : R
}

impl<R> Reader<R> where R : Read {
    pub(crate) fn new(r : R) -> Self {
        Self {
            rdr: r
        }
    }

    pub(crate) fn read_u32_le(&mut self) -> crate::Result<u32> {
        let mut b = [0_u8; 4];
        self.rdr.read_exact(&mut b)?;
        return Ok(u32::from_le_bytes(b))
    }

    pub(crate) fn read_u64_le(&mut self) -> crate::Result<u64> {
        let mut b = [0_u8; 8];
        self.rdr.read_exact(&mut b)?;
        return Ok(u64::from_le_bytes(b))
    }

    pub(crate) fn read_exact(&mut self, len : usize) -> crate::Result<Vec<u8>> {
        let mut b = vec![0; len];
        self.rdr.read_exact(&mut b)?;
        return Ok(b)
    }
}