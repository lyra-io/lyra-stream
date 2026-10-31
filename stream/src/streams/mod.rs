//! Page-cache stream operations.
//!
//! The stream layer owns only the page-cache data segments and the logical
//! index. Durability is delegated to the WAL, which owns its own checkpoint
//! and asks the stream for the commit offset when it needs to trim.

mod error;
mod page_cache;
mod snapshot;

pub use error::StreamError;
pub use page_cache::{PageCacheStream, StreamOptions};

use bytes::Bytes;

/// Operations exposed by a stream storage implementation.
pub trait StreamStorage: Send + Sync {
    fn append(&self, stream_id: i64, payload: Bytes) -> Result<u64, StreamError>;

    fn read(&self, stream_id: i64, offset: u64) -> Option<Bytes>;

    fn read_range(&self, stream_id: i64, start: i64, end: i64) -> Vec<(u64, Bytes)>;

    fn checkpoint(&self) -> Result<u64, StreamError>;

    fn commit_offset(&self) -> u64;

    fn record_count(&self) -> usize;
}
