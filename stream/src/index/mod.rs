//! Logical `(stream_id, offset)` index storage.
//!
//! The index maps a stream and its logical offset to the physical position of
//! the record in the segment set. It is deliberately abstracted behind a trait
//! so the page-cache implementation can start with an in-memory index and later
//! swap in a faster on-disk or skiplist-backed implementation.

mod error;
mod memory;

pub use error::IndexError;
pub use memory::MemoryIndex;

use crate::segment::Position;

/// Key for one record in a stream.
pub type IndexKey = (i64, u64);

/// Abstract ordered index.
pub trait IndexStorage: Send + Sync {
    fn put(&self, stream_id: i64, offset: u64, position: Position) -> Result<(), IndexError>;

    fn get(&self, stream_id: i64, offset: u64) -> Result<Option<Position>, IndexError>;

    fn range(
        &self,
        stream_id: i64,
        start: u64,
        end: u64,
    ) -> Result<Vec<(u64, Position)>, IndexError>;

    fn entries(&self) -> Result<Vec<(IndexKey, Position)>, IndexError>;

    fn len(&self) -> usize;

    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}
