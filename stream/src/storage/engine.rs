//! The storage coordinator: WAL + mmap'd segments + in-memory index.

use crate::storage::checkpoint::Checkpoint;
use crate::storage::error::StorageError;
use crate::storage::index::RecordIndex;
use crate::storage::segment::SegmentSet;
use crate::storage::shared_wal::{SharedWal, WalRecord};
use bytes::Bytes;
use std::path::{Path, PathBuf};
use std::sync::RwLock;

const DEFAULT_SEGMENT_SIZE: usize = 64 * 1024 * 1024;

/// Configuration for a [`Storage`] instance.
#[derive(Clone, Copy, Debug)]
pub struct StorageOptions {
    /// Fixed size of each mmap'd data segment.
    pub segment_size: usize,
}

impl Default for StorageOptions {
    fn default() -> Self {
        Self {
            segment_size: DEFAULT_SEGMENT_SIZE,
        }
    }
}

struct Inner {
    segments: SegmentSet,
    index: RecordIndex,
    checkpoint_offset: u64,
    highest_offset: u64,
}

/// The standalone stream storage engine.
///
/// Appends are serialized through the WAL writer and an internal write lock.
/// Reads take the read lock and are served directly from the page cache.
pub struct Storage {
    wal: SharedWal,
    inner: RwLock<Inner>,
    checkpoint_path: PathBuf,
    index_path: PathBuf,
}

impl Storage {
    pub fn open(dir: impl AsRef<Path>, options: StorageOptions) -> Result<Self, StorageError> {
        let dir = dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&dir)?;

        let checkpoint_path = dir.join("checkpoint");
        let index_path = dir.join("index.snap");
        let wal_path = dir.join("wal.log");
        let checkpoint = Checkpoint::read(&checkpoint_path)?;
        let wal_records = SharedWal::read_records(&wal_path)?;
        let wal_max = wal_records
            .iter()
            .map(|record| record.offset)
            .max()
            .unwrap_or(0);
        let start_sequence = wal_max.max(checkpoint.commit_offset.saturating_sub(1));
        let wal = SharedWal::open(&wal_path, start_sequence)?;

        let segments = SegmentSet::open(&dir, options.segment_size, &checkpoint)?;
        let mut inner = Inner {
            segments,
            index: RecordIndex::default(),
            checkpoint_offset: checkpoint.commit_offset,
            highest_offset: start_sequence,
        };
        inner.index = RecordIndex::read_snapshot(&index_path, &checkpoint, &inner.segments)?;
        replay_wal_tail(&mut inner, wal_records);

        Ok(Self {
            wal,
            inner: RwLock::new(inner),
            checkpoint_path,
            index_path,
        })
    }

    pub fn append(&self, stream_id: i64, payload: Bytes) -> Result<u64, StorageError> {
        let offset = self.wal.append(stream_id, payload.clone())?;
        let mut inner = self.inner.write().expect("storage lock poisoned");
        let position = inner.segments.append(&payload)?;
        inner.index.insert(stream_id, offset, position);
        inner.highest_offset = inner.highest_offset.max(offset);
        Ok(offset)
    }

    pub fn read(&self, stream_id: i64, offset: u64) -> Option<Bytes> {
        let inner = self.inner.read().expect("storage lock poisoned");
        let position = inner.index.get(stream_id, offset)?;
        Some(Bytes::copy_from_slice(inner.segments.read(position)))
    }

    pub fn read_range(&self, stream_id: i64, start: i64, end: i64) -> Vec<(u64, Bytes)> {
        let inner = self.inner.read().expect("storage lock poisoned");
        let mut records: Vec<_> = inner
            .index
            .iter()
            .filter_map(|((sid, offset), position)| {
                if sid != stream_id || offset < start.max(0) as u64 || offset >= end.max(0) as u64 {
                    return None;
                }
                Some((
                    offset,
                    Bytes::copy_from_slice(inner.segments.read(position)),
                ))
            })
            .collect();
        records.sort_unstable_by_key(|(offset, _)| *offset);
        records
    }

    pub fn checkpoint(&self) -> Result<u64, StorageError> {
        let mut inner = self.inner.write().expect("storage lock poisoned");
        inner.segments.sync()?;
        let commit_offset = inner.highest_offset + 1;
        inner.index.write_snapshot(&self.index_path)?;

        let (next_segment_id, active_segment_id, active_written) = inner.segments.layout();
        let checkpoint = Checkpoint {
            commit_offset,
            next_segment_id,
            active_segment_id: active_segment_id as u64,
            active_written,
        };
        checkpoint.write(&self.checkpoint_path)?;
        self.wal.trim(commit_offset)?;
        inner.checkpoint_offset = commit_offset;
        Ok(commit_offset)
    }

    pub fn commit_offset(&self) -> u64 {
        self.inner
            .read()
            .expect("storage lock poisoned")
            .checkpoint_offset
    }

    pub fn record_count(&self) -> usize {
        self.inner
            .read()
            .expect("storage lock poisoned")
            .index
            .len()
    }
}

fn replay_wal_tail(inner: &mut Inner, records: Vec<WalRecord>) {
    for record in records {
        if record.offset < inner.checkpoint_offset {
            continue;
        }
        let payload = Bytes::from(record.payload);
        match inner.segments.append(&payload) {
            Ok(position) => {
                inner
                    .index
                    .insert(record.stream_id, record.offset, position);
                inner.highest_offset = inner.highest_offset.max(record.offset);
            }
            Err(error) => {
                tracing::error!(%error, offset = record.offset, "WAL replay append failed");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn appends_reads_checkpoints_and_recovers() {
        let dir = tempfile::tempdir().unwrap();
        let options = StorageOptions { segment_size: 32 };

        let storage = Arc::new(Storage::open(dir.path(), options).unwrap());
        let offsets: Vec<(i64, u64, Vec<u8>)> = (0..20u64)
            .map(|i| {
                let stream_id = (i % 2) as i64;
                let payload = format!("rec-{i}").into_bytes();
                let offset = storage
                    .append(stream_id, Bytes::from(payload.clone()))
                    .unwrap();
                (stream_id, offset, payload)
            })
            .collect();

        for (stream_id, offset, payload) in &offsets {
            assert_eq!(
                storage.read(*stream_id, *offset),
                Some(Bytes::from(payload.clone()))
            );
        }

        let checkpoint = storage.checkpoint().unwrap();
        assert!(checkpoint > 0);
        assert_eq!(storage.commit_offset(), checkpoint);

        let post = storage
            .append(7, Bytes::from_static(b"post-checkpoint"))
            .unwrap();
        drop(storage);

        let reopened = Arc::new(Storage::open(dir.path(), options).unwrap());
        for (stream_id, offset, payload) in &offsets {
            assert_eq!(
                reopened.read(*stream_id, *offset),
                Some(Bytes::from(payload.clone())),
                "pre-checkpoint record survived"
            );
        }
        assert_eq!(
            reopened.read(7, post),
            Some(Bytes::from_static(b"post-checkpoint"))
        );
    }

    #[test]
    fn read_range_returns_offsets_in_order() {
        let dir = tempfile::tempdir().unwrap();
        let storage = Storage::open(dir.path(), StorageOptions::default()).unwrap();
        for i in 0..5u64 {
            storage
                .append(1, Bytes::from(format!("event-{i}").into_bytes()))
                .unwrap();
        }

        let records = storage.read_range(1, 1, 4);
        let offsets: Vec<u64> = records.iter().map(|(offset, _)| *offset).collect();
        assert_eq!(offsets, vec![1, 2, 3]);
    }
}
