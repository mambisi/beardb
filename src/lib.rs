
use crate::error::Error;

mod cmp;
mod codec;
mod error;
mod iter;
mod skiplist;
mod types;
mod memtable;
mod log;
mod sst;

pub type Result<T> = std::result::Result<T, Error>;

#[macro_export]
macro_rules! ensure {
    ($cond:expr,$err:expr $(,)?) => {
        if !$cond {
            return Err($err);
        }
    };
}
