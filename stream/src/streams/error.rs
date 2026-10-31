//! Errors produced by stream storage implementations.

use crate::index::IndexError;
use crate::wal::SharedWalError;

#[derive(Debug, thiserror::Error)]
pub enum StreamError {
    #[error("stream I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("stream WAL error: {0}")]
    Wal(#[from] SharedWalError),

    #[error("stream index error: {0}")]
    Index(#[from] IndexError),

    #[error("corrupt stream metadata: {0}")]
    Corrupt(String),
}
