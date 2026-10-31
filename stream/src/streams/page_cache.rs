//! Page-cache stream storage implementation.

use super::snapshot::{self, SnapshotEntry};
use super::{StreamError, StreamStorage};
use crate::index::{IndexStorage, MemoryIndex};
use crate::segment::SegmentSet;
use crate::vfs::{StandardVfs, Vfs, VfsI};
use crate::wal::{SharedWal, WalRecord};
use bytes::Bytes;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

const DEFAULT_SEGMENT_SIZE: usize = 64 * 1024 * 1024;

#[derive(Clone, Copy, Debug)]
pub struct StreamOptions {
    pub segment_size: usize,
}

impl Default for StreamOptions {
    fn default() -> Self {
        Self {
            segment_size: DEFAULT_SEGMENT_SIZE,
        }
    }
}

/// Page-cache stream storage: WAL durability + raw segments + in-memory index.
pub struct PageCacheStream {
    wal: SharedWal,
    segments: RwLock<SegmentSet>,
    index: Arc<dyn IndexStorage>,
    vfs: VfsI,
    index_path: PathBuf,
    highest_offset: AtomicU64,
}

impl PageCacheStream {
    pub fn open(
        vfs: VfsI,
        dir: impl AsRef<Path>,
        options: StreamOptions,
    ) -> Result<Self, StreamError> {
        let dir = dir.as_ref().to_path_buf();
        vfs.create_dir(&dir)?;
        let wal_dir = dir.join("wal");
        let segments_dir = dir.join("segments");
        let index_path = dir.join("index.snap");

        let wal_records = SharedWal::read_records_at(&vfs, &wal_dir)?;
        let wal_max = wal_records
            .iter()
            .map(|record| record.offset)
            .max()
            .unwrap_or(0);
        let commit_offset = SharedWal::commit_offset_at(&vfs, &wal_dir)?;
        let start_sequence = wal_max.max(commit_offset.saturating_sub(1));
        let wal = SharedWal::open(vfs.clone(), &wal_dir, start_sequence)?;

        let segments =
            SegmentSet::recover(vfs.clone(), &segments_dir, options.segment_size as u64)?;
        let index: Arc<dyn IndexStorage> = Arc::new(MemoryIndex::new());
        let snapshot_entries = snapshot::read(&vfs, &index_path)?;
        for ((stream_id, offset), position) in snapshot_entries {
            if offset < commit_offset && segments.validate(position) {
                index.put(stream_id, offset, position)?;
            }
        }

        let stream = Self {
            wal,
            segments: RwLock::new(segments),
            index,
            vfs,
            index_path,
            highest_offset: AtomicU64::new(start_sequence),
        };
        stream.replay_tail(wal_records, commit_offset)?;
        Ok(stream)
    }

    fn replay_tail(&self, records: Vec<WalRecord>, commit_offset: u64) -> Result<(), StreamError> {
        let mut segments = self.segments.write().expect("segment lock poisoned");
        for record in records {
            if record.offset < commit_offset {
                continue;
            }
            let payload = Bytes::from(record.payload);
            let position = segments.append(&payload)?;
            self.index.put(record.stream_id, record.offset, position)?;
            self.highest_offset
                .fetch_max(record.offset, Ordering::SeqCst);
        }
        Ok(())
    }

    fn snapshot_entries(&self) -> Result<Vec<SnapshotEntry>, StreamError> {
        Ok(self.index.entries()?)
    }
}

impl StreamStorage for PageCacheStream {
    fn append(&self, stream_id: i64, payload: Bytes) -> Result<u64, StreamError> {
        let offset = self.wal.append(stream_id, payload.clone())?;
        let position = {
            let mut segments = self.segments.write().expect("segment lock poisoned");
            segments.append(&payload)?
        };
        self.index.put(stream_id, offset, position)?;
        self.highest_offset.fetch_max(offset, Ordering::SeqCst);
        Ok(offset)
    }

    fn read(&self, stream_id: i64, offset: u64) -> Option<Bytes> {
        let position = self.index.get(stream_id, offset).ok().flatten()?;
        let segments = self.segments.read().expect("segment lock poisoned");
        segments.read(position).ok()
    }

    fn read_range(&self, stream_id: i64, start: i64, end: i64) -> Vec<(u64, Bytes)> {
        let start = start.max(0) as u64;
        let end = end.max(0) as u64;
        let Ok(records) = self.index.range(stream_id, start, end) else {
            return Vec::new();
        };
        let segments = self.segments.read().expect("segment lock poisoned");
        records
            .into_iter()
            .filter_map(|(offset, position)| {
                segments
                    .read(position)
                    .ok()
                    .map(|payload| (offset, payload))
            })
            .collect()
    }

    fn checkpoint(&self) -> Result<u64, StreamError> {
        {
            let segments = self.segments.read().expect("segment lock poisoned");
            segments.sync()?;
        }
        let entries = self.snapshot_entries()?;
        snapshot::write(&self.vfs, &self.index_path, &entries)?;

        let commit_offset = self.highest_offset.load(Ordering::SeqCst) + 1;
        self.wal.checkpoint(|| commit_offset)?;
        Ok(commit_offset)
    }

    fn commit_offset(&self) -> u64 {
        self.wal.commit_offset().unwrap_or_else(|error| {
            tracing::error!(error = %error, "failed to read WAL commit offset");
            0
        })
    }

    fn record_count(&self) -> usize {
        self.index.len()
    }
}

/// Convenience constructor for the standard filesystem VFS.
impl PageCacheStream {
    pub fn open_standard(
        dir: impl AsRef<Path>,
        options: StreamOptions,
    ) -> Result<Self, StreamError> {
        Self::open(VfsI::Standard(StandardVfs), dir, options)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn appends_reads_checkpoints_and_recovers() {
        let dir = tempfile::tempdir().unwrap();
        let options = StreamOptions { segment_size: 32 };

        let storage = PageCacheStream::open_standard(dir.path(), options).unwrap();
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

        let reopened = PageCacheStream::open_standard(dir.path(), options).unwrap();
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
        let storage = PageCacheStream::open_standard(dir.path(), StreamOptions::default()).unwrap();
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
