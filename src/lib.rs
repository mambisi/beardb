#![feature(slice_pattern)]
extern crate core;

use crate::error::Error;

mod bloom;
mod cmp;
mod codec;
mod error;
mod iter;
mod lfu_cache;
mod log;
mod memtable;
mod metadata;
mod skiplist;
mod table;
mod types;
mod block;
mod table_index;
mod constant;
mod table_builder;

pub type Result<T> = std::result::Result<T, Error>;

#[macro_export]
macro_rules! ensure {
    ($cond:expr,$err:expr $(,)?) => {
        if !$cond {
            return Err($err);
        }
    };
}
