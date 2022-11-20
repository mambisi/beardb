use core::slice::SlicePattern;
use std::fmt::Write;
use std::io::Read;
use std::path::{Path, PathBuf};

use memmap2::Mmap;

use crate::{Error, Result};

pub trait RandomAccess {
    fn read(&self, off: usize, len: usize) -> Result<&[u8]>;
}

impl RandomAccess for Mmap {
    fn read(&self, off: usize, len: usize) -> Result<&[u8]> {
        if off + len > self.len() {
            return Err(Error::IOError("failed to read buffer".to_string()));
        }
        Ok(&self.as_slice()[off..off + len])
    }
}

pub struct FileLock {
    pub id: String,
}

pub trait Env {
    fn open_sequential_file(&self, _: &Path) -> Result<Box<dyn Read>>;
    fn open_random_access_file(&self, _: &Path) -> Result<Box<dyn RandomAccess>>;
    fn open_writable_file(&self, _: &Path) -> Result<Box<dyn Write>>;
    fn open_appendable_file(&self, _: &Path) -> Result<Box<dyn Write>>;

    fn exists(&self, _: &Path) -> Result<bool>;
    fn children(&self, _: &Path) -> Result<Vec<PathBuf>>;
    fn size_of(&self, _: &Path) -> Result<usize>;

    fn delete(&self, _: &Path) -> Result<()>;
    fn mkdir(&self, _: &Path) -> Result<()>;
    fn rmdir(&self, _: &Path) -> Result<()>;
    fn rename(&self, _: &Path, _: &Path) -> Result<()>;

    fn lock(&self, _: &Path) -> Result<FileLock>;
    fn unlock(&self, l: FileLock) -> Result<()>;
}