//! Fixed-size mmap'd data segments served by the page cache.

use crate::storage::checkpoint::Checkpoint;
use crate::storage::error::StorageError;
use memmap2::MmapMut;
use std::fs::OpenOptions;
use std::path::{Path, PathBuf};

/// Physical location of a record inside the mmap'd data store.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Position {
    pub segment: usize,
    pub offset: usize,
    pub len: u32,
}

/// A fixed-size, mmap'd segment file. Writes are `memcpy` into the mapping and
/// reads are zero-copy slices into the same mapping.
pub struct Segment {
    map: MmapMut,
    written: usize,
    capacity: usize,
}

impl Segment {
    pub fn open(
        dir: &Path,
        id: u64,
        capacity: usize,
        written: usize,
    ) -> Result<Self, StorageError> {
        let path = dir.join(format!("{id:010}.seg"));
        let file = OpenOptions::new().read(true).write(true).open(&path)?;
        if file.metadata()?.len() < capacity as u64 {
            file.set_len(capacity as u64)?;
        }
        // SAFETY: the file is owned by this process and never truncated while
        // mapped. Fixed-size segment files avoid SIGBUS from external truncation.
        let map = unsafe { MmapMut::map_mut(&file)? };
        Ok(Self {
            map,
            written,
            capacity,
        })
    }

    pub fn create(dir: &Path, id: u64, capacity: usize) -> Result<Self, StorageError> {
        let path = dir.join(format!("{id:010}.seg"));
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(true)
            .open(path)?;
        file.set_len(capacity as u64)?;
        // SAFETY: the file is owned by this process and never truncated while
        // mapped.
        let map = unsafe { MmapMut::map_mut(&file)? };
        Ok(Self {
            map,
            written: 0,
            capacity,
        })
    }

    pub fn remaining(&self) -> usize {
        self.capacity - self.written
    }

    pub fn written(&self) -> usize {
        self.written
    }

    pub fn read(&self, offset: usize, len: u32) -> &[u8] {
        &self.map[offset..offset + len as usize]
    }

    pub fn sync(&self) -> Result<(), StorageError> {
        self.map.flush()?;
        Ok(())
    }
}

/// The ordered collection of data segments owned by a [`crate::storage::Storage`].
pub struct SegmentSet {
    dir: PathBuf,
    segments: Vec<Segment>,
    active: usize,
    segment_size: usize,
    next_segment_id: u64,
}

impl SegmentSet {
    pub fn open(
        dir: &Path,
        segment_size: usize,
        checkpoint: &Checkpoint,
    ) -> Result<Self, StorageError> {
        let mut segments = Vec::new();
        for id in 0..checkpoint.next_segment_id {
            let written = if id == checkpoint.active_segment_id {
                checkpoint.active_written
            } else {
                segment_size
            };
            segments.push(Segment::open(dir, id, segment_size, written)?);
        }

        let active = if segments.is_empty() {
            segments.push(Segment::create(dir, 0, segment_size)?);
            0
        } else {
            let active = checkpoint.active_segment_id as usize;
            if active >= segments.len() {
                return Err(StorageError::Corrupt(format!(
                    "checkpoint active segment {active} exceeds segment count {}",
                    segments.len()
                )));
            }
            active
        };

        let next_segment_id = segments.len() as u64;
        Ok(Self {
            dir: dir.to_path_buf(),
            segments,
            active,
            segment_size,
            next_segment_id,
        })
    }

    pub fn append(&mut self, payload: &[u8]) -> Result<Position, StorageError> {
        if self.segments[self.active].remaining() < payload.len() {
            let segment = Segment::create(&self.dir, self.next_segment_id, self.segment_size)?;
            self.segments.push(segment);
            self.active = self.segments.len() - 1;
            self.next_segment_id += 1;
        }

        let segment = self.active;
        let offset = self.segments[segment].written;
        let len = payload.len() as u32;
        self.segments[segment].map[offset..offset + payload.len()].copy_from_slice(payload);
        self.segments[segment].written += payload.len();
        Ok(Position {
            segment,
            offset,
            len,
        })
    }

    pub fn read(&self, position: Position) -> &[u8] {
        self.segments[position.segment].read(position.offset, position.len)
    }

    pub fn validate(&self, position: Position) -> bool {
        position.segment < self.segments.len()
            && position.offset + position.len as usize <= self.segments[position.segment].written
    }

    pub fn sync(&self) -> Result<(), StorageError> {
        for segment in &self.segments {
            segment.sync()?;
        }
        Ok(())
    }

    pub fn layout(&self) -> (u64, usize, usize) {
        (
            self.next_segment_id,
            self.active,
            self.segments[self.active].written,
        )
    }
}
