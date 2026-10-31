//! The durable recovery point for stream storage.

use crate::storage::error::StorageError;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;

/// A checkpoint describes where the data store is durable and where appends
/// resume after a restart.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Checkpoint {
    /// Records with a lower offset are durable in the data store and may be
    /// trimmed from the WAL.
    pub commit_offset: u64,
    /// Number of segment files that exist at the checkpoint.
    pub next_segment_id: u64,
    /// The segment that was being appended to when the checkpoint was taken.
    pub active_segment_id: u64,
    /// Bytes written into the active segment at the checkpoint.
    pub active_written: usize,
}

impl Checkpoint {
    pub fn read(path: &Path) -> Result<Self, StorageError> {
        match std::fs::read_to_string(path) {
            Ok(contents) => {
                let mut fields = contents.split_whitespace();
                Ok(Self {
                    commit_offset: fields.next().and_then(|v| v.parse().ok()).unwrap_or(0),
                    next_segment_id: fields.next().and_then(|v| v.parse().ok()).unwrap_or(0),
                    active_segment_id: fields.next().and_then(|v| v.parse().ok()).unwrap_or(0),
                    active_written: fields.next().and_then(|v| v.parse().ok()).unwrap_or(0),
                })
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(Self::default()),
            Err(error) => Err(StorageError::Io(error)),
        }
    }

    pub fn write(&self, path: &Path) -> Result<(), StorageError> {
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(path)?;
        writeln!(
            file,
            "{} {} {} {}",
            self.commit_offset, self.next_segment_id, self.active_segment_id, self.active_written
        )?;
        file.sync_data()?;
        Ok(())
    }
}
