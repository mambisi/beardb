#![feature(slice_pattern)]
#![feature(generic_associated_types)]
#![feature(map_first_last)]
#![feature(default_free_fn)]
#![feature(build_hasher_simple_hash_one)]
#![feature(let_else)]

extern crate core;
extern crate core;

use crate::error::Error;

mod block;
mod bloom;
mod cmp;
mod codec;
mod constant;
mod error;
mod iter;
mod log;
mod memtable;
mod memtable_cache;
mod metadata;
mod skiplist;
mod table;
mod table_builder;
mod table_index;
mod types;
mod env;
mod disk_env;
mod chunk;

pub type Result<T> = std::result::Result<T, Error>;

#[macro_export]
macro_rules! ensure {
    ($cond:expr,$err:expr $(,)?) => {
        if !$cond {
            return Err($err);
        }
    };
}
