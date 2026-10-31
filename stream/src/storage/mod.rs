//! Standalone stream storage.
//!
//! The storage engine is split across focused modules:
//!
//! - [`shared_wal`]: the group-committed write-ahead log, which is the single
//!   durability boundary and is read only during recovery.
//! - [`segment`]: fixed-size mmap'd data segments served by the page cache.
//! - [`index`]: the in-memory `(stream_id, offset) -> position` index and its
//!   durable snapshot.
//! - [`checkpoint`]: the durable recovery point that ties the index snapshot,
//!   segment layout and WAL trim offset together.
//! - [`storage`]: the coordinator that exposes append/read/checkpoint.

pub mod checkpoint;
pub mod engine;
pub mod error;
pub mod index;
pub mod segment;
pub mod shared_wal;

pub use engine::{Storage, StorageOptions};
pub use error::StorageError;
