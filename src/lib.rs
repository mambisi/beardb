use crate::error::Error;

mod cmp;
mod codec;
mod error;
mod iter;
mod skiplist;
mod types;

pub type Result<T> = std::result::Result<T, Error>;
