use std::io;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("io error {0}")]
    IOError(#[from] io::Error),
    #[error("unknown data store error")]
    Unknown,

    // skip list errors
    #[error("invalid iterator")]
    InvalidIterator,
    #[error("duplicate entry")]
    DuplicateEntry,

    #[error("error encoding or decoding")]
    CodecError,
}