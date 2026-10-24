//! Stateful write-ahead log API.

mod error;
mod log;
mod log_syncer;
mod log_writer;
mod ops;
mod options;
mod segment;

pub use error::WalError;
pub use log::SegmentLog;
pub use options::LogOptions;

use async_trait::async_trait;
use bytes::Bytes;
use crate::vfs::{StandardVfs, VfsI};
use lyra_meta::utils::promise::Promise;
use std::path::Path;

/// A monotonically increasing WAL record identifier.
pub type Sequence = u64;

/// Maximum encoded WAL segment size before rotation.
pub(crate) const WAL_SEGMENT_SIZE: u64 = 64 * 1024 * 1024;

/// Maximum number of appends buffered while the writer is busy.
pub(crate) const MAX_INFLIGHT_APPEND_NUM: usize = 4096;

/// Replays durable WAL records in sequence order.
///
/// Each returned tuple is `(sequence, envelope)` where `envelope` is the
/// caller-supplied payload that was appended after the 8-byte sequence prefix.
/// The caller owns the envelope format (for example, a stream id followed by
/// the record payload) and is responsible for decoding it.
pub fn replay(dir: &Path) -> Result<Vec<(Sequence, Bytes)>, WalError> {
    use self::segment::Segment;

    let vfs = VfsI::Standard(StandardVfs);
    let segments = segment::list_segments(&vfs, dir)?;
    let segment_count = segments.len();
    let mut records = Vec::new();

    for (index, (file_number, path)) in segments.into_iter().enumerate() {
        let file_size = std::fs::metadata(&path)?.len();
        if file_size > WAL_SEGMENT_SIZE {
            return Err(WalError::Corruption {
                path: path.clone(),
                message: format!(
                    "segment size {} exceeds the maximum {WAL_SEGMENT_SIZE}",
                    file_size
                ),
            });
        }

        let segment = segment::FileSegment::open(
            &vfs,
            &path,
            file_number,
            WAL_SEGMENT_SIZE,
            file_size,
        )?;
        let final_segment = index + 1 == segment_count;
        let mut position = 0u64;
        while position < file_size {
            let (next_position, chunk) = match segment.read(position, WAL_SEGMENT_SIZE as usize) {
                Ok(read) => read,
                Err(WalError::Truncated { .. }) if final_segment => break,
                Err(error) => return Err(error),
            };
            if chunk.is_empty() {
                return Err(WalError::Worker(
                    "WAL segment read made no replay progress".into(),
                ));
            }
            for record in chunk {
                if record.len() < 8 {
                    return Err(WalError::Corruption {
                        path: path.clone(),
                        message: "replayed record shorter than its sequence prefix".into(),
                    });
                }
                let sequence = Sequence::from_le_bytes(record[..8].try_into().unwrap());
                records.push((sequence, Bytes::copy_from_slice(&record[8..])));
            }
            position = next_position;
        }
    }

    Ok(records)
}

/// An ordered, durable stream log.
#[async_trait]
pub trait Log: Send + Sync {
    /// Eagerly submits `payload` and returns its eventual assigned sequence.
    ///
    /// A successfully returned promise remains independent of its waiter and
    /// follows the log's configured synchronization policy.
    fn append(&self, payload: Bytes) -> Promise<Sequence, WalError>;

    /// Stops admission, drains owned work, and closes all components.
    async fn close(&self);
}

/// Whether the log still accepts appends.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Lifecycle {
    /// The log is open and accepts appends.
    Running,
    /// Close has begun and new appends are rejected.
    Closing,
    /// All owned workers and components have been closed.
    Closed,
}
