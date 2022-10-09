

use crate::error::Error;

mod skiplist;
mod iter;
mod error;
mod types;
mod cmp;
mod codec;

pub type Result<T> = std::result::Result<T, Error>;