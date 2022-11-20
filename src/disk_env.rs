use std::collections::HashMap;
use std::fmt::Write;
use std::fs;
use std::fs::{File, OpenOptions};
use std::io::{ErrorKind, Read};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use fs2::FileExt;
use memmap2::Mmap;
use parking_lot::Mutex;

use crate::env::{Env, FileLock, RandomAccess};

pub(crate) struct PosixDiskEnv {
    locks: Arc<Mutex<HashMap<String, File>>>,
}

// impl Env for PosixDiskEnv {
//     fn open_sequential_file(&self, path: &Path) -> crate::Result<Box<dyn Read>> {
//         Ok(Box::new(
//             OpenOptions::new()
//                 .read(true)
//                 .open(p)
//                 .map_err(|e| map_err_with_name("open (seq)", p, e))?,
//         ))
//     }
//
//     fn open_random_access_file(&self, _: &Path) -> crate::Result<Box<dyn RandomAccess>> {
//         Ok(OpenOptions::new()
//             .read(true)
//             .open(p)
//             .map(|f| unsafe {
//                 let mmap = Mmap::map(f)?;
//                 let b: Box<dyn RandomAccess> = Box::new(mmap);
//                 b
//             })
//             .map_err(|e| map_err_with_name("open (randomaccess)", p, e))?)
//     }
//
//     fn open_writable_file(&self, _: &Path) -> crate::Result<Box<dyn Write>> {
//         Ok(Box::new(
//             OpenOptions::new()
//                 .create(true)
//                 .write(true)
//                 .append(false)
//                 .open(p)
//                 .map_err(|e| map_err_with_name("open (write)", p, e))?,
//         ))
//     }
//
//     fn open_appendable_file(&self, _: &Path) -> crate::Result<Box<dyn Write>> {
//         Ok(Box::new(
//             OpenOptions::new()
//                 .create(true)
//                 .write(true)
//                 .append(true)
//                 .open(p)
//                 .map_err(|e| map_err_with_name("open (append)", p, e))?,
//         ))
//     }
//
//     fn exists(&self, _: &Path) -> crate::Result<bool> {
//         Ok(p.exists())
//     }
//
//     fn children(&self, _: &Path) -> crate::Result<Vec<PathBuf>> {
//         let dir_reader = fs::read_dir(p).map_err(|e| map_err_with_name("children", p, e))?;
//         let filenames = dir_reader
//             .map(|r| match r {
//                 Ok(_) => {
//                     let direntry = r.unwrap();
//                     Path::new(&direntry.file_name()).to_owned()
//                 }
//                 Err(_) => Path::new("").to_owned(),
//             })
//             .filter(|s| !s.as_os_str().is_empty());
//         Ok(Vec::from_iter(filenames))
//     }
//
//     fn size_of(&self, _: &Path) -> crate::Result<usize> {
//         let meta = fs::metadata(p).map_err(|e| map_err_with_name("size_of", p, e))?;
//         Ok(meta.len() as usize)
//     }
//
//     fn delete(&self, _: &Path) -> crate::Result<()> {
//         Ok(fs::remove_file(p).map_err(|e| map_err_with_name("delete", p, e))?)
//     }
//
//     fn mkdir(&self, _: &Path) -> crate::Result<()> {
//         Ok(fs::create_dir_all(p).map_err(|e| map_err_with_name("mkdir", p, e))?)
//     }
//
//     fn rmdir(&self, p: &Path) -> crate::Result<()> {
//         Ok(fs::remove_dir_all(p).map_err(|e| map_err_with_name("rmdir", p, e))?)
//     }
//     fn rename(&self, old: &Path, new: &Path) -> crate::Result<()> {
//         Ok(fs::rename(old, new).map_err(|e| map_err_with_name("rename", old, e))?)
//     }
//
//     fn lock(&self, p: &Path) -> crate::Result<FileLock> {
//         let mut locks = self.locks.lock().unwrap();
//
//         if locks.contains_key(&p.to_str().unwrap().to_string()) {
//             Err(Status::new(StatusCode::AlreadyExists, "Lock is held"))
//         } else {
//             let f = OpenOptions::new()
//                 .write(true)
//                 .create(true)
//                 .open(p)
//                 .map_err(|e| map_err_with_name("lock", p, e))?;
//
//             match f.try_lock_exclusive() {
//                 Err(err) if err.kind() == ErrorKind::WouldBlock => {
//                     return Err(Status::new(
//                         StatusCode::LockError,
//                         "lock on database is already held by different process",
//                     ))
//                 }
//                 Err(_) => {
//                     return Err(Status::new(
//                         StatusCode::Errno(errno::errno()),
//                         &format!("unknown lock error on file {:?} (file {})", f, p.display()),
//                     ))
//                 }
//                 _ => (),
//             };
//
//             locks.insert(p.to_str().unwrap().to_string(), f);
//             let lock = FileLock {
//                 id: p.to_str().unwrap().to_string(),
//             };
//             Ok(lock)
//         }
//     }
//     fn unlock(&self, l: FileLock) -> crate::Result<()> {
//         let mut locks = self.locks.lock().unwrap();
//         if !locks.contains_key(&l.id) {
//             err(
//                 StatusCode::LockError,
//                 &format!("unlocking a file that is not locked: {}", l.id),
//             )
//         } else {
//             let f = locks.remove(&l.id).unwrap();
//             if f.unlock().is_err() {
//                 return err(StatusCode::LockError, &format!("unlock failed: {}", l.id));
//             }
//             Ok(())
//         }
//     }
// }
