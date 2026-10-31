//! One raw, append-only segment file.

use crate::vfs::{IoFile, OpenOptions, Vfs, VfsFile, VfsI};
use bytes::Bytes;
use std::io::Result;
use std::path::Path;

/// A single-writer append-only segment file.
pub struct Segment {
    file: VfsFile,
    max_size: u64,
    write_position: u64,
}

impl Segment {
    pub fn create(vfs: &VfsI, path: &Path, max_size: u64) -> Result<Self> {
        let file = vfs.open(path, OpenOptions::CreateNew)?;
        Ok(Self {
            file,
            max_size,
            write_position: 0,
        })
    }

    pub fn open(vfs: &VfsI, path: &Path, max_size: u64) -> Result<Self> {
        let file = vfs.open(path, OpenOptions::Existing)?;
        let write_position = file.size()?;
        Ok(Self {
            file,
            max_size,
            write_position,
        })
    }

    pub fn append(&mut self, bytes: &[u8]) -> Result<u64> {
        let length = u64::try_from(bytes.len()).map_err(|_| {
            std::io::Error::new(std::io::ErrorKind::InvalidInput, "buffer too large")
        })?;
        if self.write_position.checked_add(length).ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "segment position overflow",
            )
        })? > self.max_size
        {
            return Err(std::io::Error::new(
                std::io::ErrorKind::WriteZero,
                "segment is full",
            ));
        }
        let position = self.file.append(bytes)?;
        self.write_position += length;
        Ok(position)
    }

    pub fn read_at(&self, position: u64, length: usize) -> Result<Bytes> {
        self.file.read_at(position, length)
    }

    pub fn truncate(&mut self, size: u64) -> Result<()> {
        self.file.truncate(size)?;
        self.write_position = size;
        Ok(())
    }

    pub fn sync(&self) -> Result<()> {
        self.file.sync()
    }

    pub fn write_position(&self) -> u64 {
        self.write_position
    }

    pub fn max_size(&self) -> u64 {
        self.max_size
    }

    pub fn remaining(&self) -> u64 {
        self.max_size - self.write_position
    }
}
