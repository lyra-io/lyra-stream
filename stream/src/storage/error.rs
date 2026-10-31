//! Errors produced by the standalone stream storage engine.

/// Storage-level errors.
#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("storage I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("corrupt storage metadata: {0}")]
    Corrupt(String),

    #[error("write-ahead log writer is closed")]
    WalClosed,
}
