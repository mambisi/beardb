use std::io;

use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("SendError {0}")]
    SendError(Box<dyn std::error::Error + Send + Sync>),
}