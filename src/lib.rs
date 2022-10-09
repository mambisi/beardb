extern crate core;

use crate::error::Error;

mod skiplist;
mod iter;
mod error;

pub type Result<T> = std::result::Result<T, Error>;