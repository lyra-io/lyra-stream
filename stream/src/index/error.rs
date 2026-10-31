//! Errors produced by index storage implementations.

#[derive(Debug, thiserror::Error)]
pub enum IndexError {
    #[error("index I/O error: {0}")]
    Io(#[from] std::io::Error),
}
