use std::io;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("io error {0}")]
    IOError(#[from] io::Error),
    #[error("io error {0}")]
    SkipListError(#[from] crate::skiplist::SkipListError),
    #[error("unknown data store error")]
    Unknown,
}