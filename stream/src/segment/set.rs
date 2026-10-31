//! An ordered set of raw segments with rotation.

use super::file::Segment;
use super::{Position, list_segments, make_segment_path};
use crate::vfs::{Vfs, VfsI};
use bytes::Bytes;
use std::io::Result;
use std::path::{Path, PathBuf};

/// Owns the data segments of one stream store or WAL.
pub struct SegmentSet {
    vfs: VfsI,
    dir: PathBuf,
    segments: Vec<Segment>,
    active: usize,
    segment_size: u64,
    next_segment_id: u64,
}

impl SegmentSet {
    pub fn recover(vfs: VfsI, dir: &Path, segment_size: u64) -> Result<Self> {
        vfs.create_dir(dir)?;
        let files = list_segments(&vfs, dir)?;
        if files.is_empty() {
            let mut set = Self {
                vfs,
                dir: dir.to_path_buf(),
                segments: Vec::new(),
                active: 0,
                segment_size,
                next_segment_id: 0,
            };
            set.rotate()?;
            return Ok(set);
        }

        for (index, (number, _)) in files.iter().enumerate() {
            if *number != index as u64 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!(
                        "segment numbers are not contiguous at {}: expected {}, got {}",
                        index, index, number
                    ),
                ));
            }
        }

        let mut segments = Vec::with_capacity(files.len());
        for (_, path) in &files {
            segments.push(Segment::open(&vfs, path, segment_size)?);
        }
        let active = segments.len() - 1;
        let next_segment_id = segments.len() as u64;
        Ok(Self {
            vfs,
            dir: dir.to_path_buf(),
            segments,
            active,
            segment_size,
            next_segment_id,
        })
    }

    fn rotate(&mut self) -> Result<()> {
        let segment = Segment::create(
            &self.vfs,
            &make_segment_path(&self.dir, self.next_segment_id),
            self.segment_size,
        )?;
        self.segments.push(segment);
        self.active = self.segments.len() - 1;
        self.next_segment_id += 1;
        Ok(())
    }

    pub fn append(&mut self, payload: &[u8]) -> Result<Position> {
        if self.segments[self.active].remaining() < payload.len() as u64 {
            self.rotate()?;
        }
        let segment = self.active as u32;
        let offset = self.segments[self.active].append(payload)?;
        let offset = u32::try_from(offset).map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "segment position exceeds u32",
            )
        })?;
        let len = u32::try_from(payload.len()).map_err(|_| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, "record length exceeds u32")
        })?;
        Ok(Position {
            segment,
            offset,
            len,
        })
    }

    pub fn read(&self, position: Position) -> Result<Bytes> {
        let segment = self
            .segments
            .get(position.segment as usize)
            .ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "segment index out of range",
                )
            })?;
        segment.read_at(position.offset as u64, position.len as usize)
    }

    pub fn sync(&self) -> Result<()> {
        for segment in &self.segments {
            segment.sync()?;
        }
        Ok(())
    }

    pub fn validate(&self, position: Position) -> bool {
        let Some(segment) = self.segments.get(position.segment as usize) else {
            return false;
        };
        position.offset as u64 + position.len as u64 <= segment.write_position()
    }

    pub fn layout(&self) -> (u64, u64, u64) {
        (
            self.next_segment_id,
            self.active as u64,
            self.segments[self.active].write_position(),
        )
    }

    pub fn segment_count(&self) -> usize {
        self.segments.len()
    }
}
