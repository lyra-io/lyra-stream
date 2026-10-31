//! In-memory index implementation.

use super::{IndexKey, IndexStorage};
use crate::index::error::IndexError;
use crate::segment::Position;
use std::collections::HashMap;
use std::sync::RwLock;

/// A simple `HashMap`-backed index protected by a read/write lock.
#[derive(Default)]
pub struct MemoryIndex {
    entries: RwLock<HashMap<IndexKey, Position>>,
}

impl MemoryIndex {
    pub fn new() -> Self {
        Self::default()
    }
}

impl IndexStorage for MemoryIndex {
    fn put(&self, stream_id: i64, offset: u64, position: Position) -> Result<(), IndexError> {
        self.entries
            .write()
            .expect("index lock poisoned")
            .insert((stream_id, offset), position);
        Ok(())
    }

    fn get(&self, stream_id: i64, offset: u64) -> Result<Option<Position>, IndexError> {
        Ok(self
            .entries
            .read()
            .expect("index lock poisoned")
            .get(&(stream_id, offset))
            .copied())
    }

    fn range(
        &self,
        stream_id: i64,
        start: u64,
        end: u64,
    ) -> Result<Vec<(u64, Position)>, IndexError> {
        let entries = self.entries.read().expect("index lock poisoned");
        let mut records: Vec<_> = entries
            .iter()
            .filter_map(|((sid, offset), position)| {
                (*sid == stream_id && *offset >= start && *offset < end)
                    .then_some((*offset, *position))
            })
            .collect();
        records.sort_unstable_by_key(|(offset, _)| *offset);
        Ok(records)
    }

    fn entries(&self) -> Result<Vec<(IndexKey, Position)>, IndexError> {
        Ok(self
            .entries
            .read()
            .expect("index lock poisoned")
            .iter()
            .map(|(key, position)| (*key, *position))
            .collect())
    }

    fn len(&self) -> usize {
        self.entries.read().expect("index lock poisoned").len()
    }
}
