use rkyv::vec::ArchivedVec;
use crate::Error;

pub(crate) struct SSTable<'a> {
    list : &'a ArchivedVec<ArchivedVec<u8>>
}





impl<'a> SSTable<'a> {
    pub(crate) fn open<B : AsRef<&'a [u8]>>(b : B) -> crate::Result<SSTable<'a>> {
        let list = rkyv::check_archived_root::<Vec<Vec<u8>>>(b.as_ref()).map_err(|e|Error::AnyError(Box::new(e)))?;
        Ok(SSTable {
            list
        })
    }
}