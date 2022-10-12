use std::io;
use thiserror::Error;

#[derive(Error, Debug)]
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

    #[error("Error {0}")]
    AnyError(Box<dyn std::error::Error + Send + Sync>),
}

impl PartialEq for Error {
    fn eq(&self, other: &Self) -> bool {
        let er_string = format!("{:?}", self);
        let other_string = format!("{:?}", other);
        er_string.eq(&other_string)
    }
}


impl From<io::Error> for Error {
    fn from(err: io::Error) -> Self {
        Self::IOError(format!("{}", err))
    }
}