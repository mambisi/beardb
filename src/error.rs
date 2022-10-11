use std::io;
use thiserror::Error;

#[derive(Error, PartialEq, Debug)]
pub enum Error {
    #[error("io error {0}")]
    IOError(String),
    #[error("unknown data store error")]
    Unknown,

    // skip list errors
    #[error("invalid iterator")]
    InvalidIterator,
    #[error("duplicate entry")]
    DuplicateEntry,

    #[error("error encoding or decoding")]
    CodecError,

    #[error("corruption {0}")]
    Corruption(String),
}


impl From<io::Error> for Error {
    fn from(err: io::Error) -> Self {
        Self::IOError(format!("{}", err))
    }
}