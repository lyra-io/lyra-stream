//! In-memory record index and its durable snapshot.

use crate::storage::checkpoint::Checkpoint;
use crate::storage::error::StorageError;
use crate::storage::segment::{Position, SegmentSet};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::Path;

const ENTRY_SIZE: usize = 28;

/// Maps `(stream_id, offset)` to the physical record position.
#[derive(Default)]
pub struct RecordIndex {
    entries: HashMap<(i64, u64), Position>,
}

impl RecordIndex {
    pub fn insert(&mut self, stream_id: i64, offset: u64, position: Position) {
        self.entries.insert((stream_id, offset), position);
    }

    pub fn get(&self, stream_id: i64, offset: u64) -> Option<Position> {
        self.entries.get(&(stream_id, offset)).copied()
    }

    pub fn iter(&self) -> impl Iterator<Item = ((i64, u64), Position)> + '_ {
        self.entries.iter().map(|(key, position)| (*key, *position))
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn write_snapshot(&self, path: &Path) -> Result<(), StorageError> {
        let tmp = path.with_extension("tmp");
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp)?;

        let mut entries: Vec<_> = self.entries.iter().collect();
        entries.sort_unstable_by_key(|((stream_id, offset), _)| (*stream_id, *offset));

        let mut buf = Vec::with_capacity(8 + entries.len() * ENTRY_SIZE);
        buf.extend_from_slice(&(entries.len() as u64).to_le_bytes());
        for ((stream_id, offset), position) in entries {
            buf.extend_from_slice(&stream_id.to_le_bytes());
            buf.extend_from_slice(&offset.to_le_bytes());
            buf.extend_from_slice(&(position.segment as u32).to_le_bytes());
            buf.extend_from_slice(&(position.offset as u32).to_le_bytes());
            buf.extend_from_slice(&position.len.to_le_bytes());
        }

        file.write_all(&buf)?;
        file.sync_data()?;
        std::fs::rename(&tmp, path)?;
        Ok(())
    }

    pub fn read_snapshot(
        path: &Path,
        checkpoint: &Checkpoint,
        segments: &SegmentSet,
    ) -> Result<Self, StorageError> {
        let mut index = Self::default();
        let mut file = match File::open(path) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(index),
            Err(error) => return Err(StorageError::Io(error)),
        };
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)?;
        if bytes.len() < 8 {
            return Ok(index);
        }

        let count = u64::from_le_bytes(bytes[0..8].try_into().unwrap()) as usize;
        let mut position = 8usize;
        for _ in 0..count {
            if position + ENTRY_SIZE > bytes.len() {
                break;
            }
            let stream_id = i64::from_le_bytes(bytes[position..position + 8].try_into().unwrap());
            let offset = u64::from_le_bytes(bytes[position + 8..position + 16].try_into().unwrap());
            let segment =
                u32::from_le_bytes(bytes[position + 16..position + 20].try_into().unwrap());
            let segment_offset =
                u32::from_le_bytes(bytes[position + 20..position + 24].try_into().unwrap());
            let len = u32::from_le_bytes(bytes[position + 24..position + 28].try_into().unwrap());
            position += ENTRY_SIZE;

            // Anything newer than the checkpoint is reconstructed from the WAL
            // tail, so only trust entries consistent with this checkpoint.
            if offset >= checkpoint.commit_offset {
                continue;
            }
            let position = Position {
                segment: segment as usize,
                offset: segment_offset as usize,
                len,
            };
            if segments.validate(position) {
                index.insert(stream_id, offset, position);
            }
        }
        Ok(index)
    }
}
