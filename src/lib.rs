#![feature(slice_pattern)]
#![feature(generic_associated_types)]
#![feature(map_first_last)]
extern crate core;

use crate::error::Error;

mod block;
mod bloom;
mod cmp;
mod codec;
mod constant;
mod error;
mod iter;
mod lfu_cache;
mod log;
mod memtable;
mod metadata;
mod skiplist;
mod table;
mod table_builder;
mod table_index;
mod types;
mod memtable_cache;

pub type Result<T> = std::result::Result<T, Error>;

#[macro_export]
macro_rules! ensure {
    ($cond:expr,$err:expr $(,)?) => {
        if !$cond {
            return Err($err);
        }
    };
}
