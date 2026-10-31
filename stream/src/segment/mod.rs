//! Shared segment files used by both the WAL and stream storage.
//!
//! A segment is just an append-only buffer file with no framing. Callers store
//! raw buffers and remember positions; recovery code owns any record framing it
//! needs on top of those buffers.

mod file;
mod set;

pub use file::Segment;
pub use set::SegmentSet;

use crate::vfs::Vfs;
use std::io::Result;
use std::path::{Path, PathBuf};

/// Physical position of a buffer inside the segment set.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Position {
    pub segment: u32,
    pub offset: u32,
    pub len: u32,
}

pub(crate) const SEGMENT_EXTENSION: &str = "seg";

pub(crate) fn make_segment_path(dir: &Path, segment_number: u64) -> PathBuf {
    dir.join(format!("{segment_number:010}.{SEGMENT_EXTENSION}"))
}

pub(crate) fn list_segments(vfs: &crate::vfs::VfsI, dir: &Path) -> Result<Vec<(u64, PathBuf)>> {
    let mut files = Vec::new();
    for path in vfs.list(dir)? {
        if path.extension().and_then(|extension| extension.to_str()) != Some(SEGMENT_EXTENSION) {
            continue;
        }
        let stem = path
            .file_stem()
            .and_then(|stem| stem.to_str())
            .ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("segment filename is not valid UTF-8: {}", path.display()),
                )
            })?;
        if stem.len() != 10 || !stem.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "segment filename must contain exactly ten digits: {}",
                    path.display()
                ),
            ));
        }
        let number = stem.parse::<u64>().map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("segment filename is not numeric: {}", path.display()),
            )
        })?;
        files.push((number, path));
    }
    files.sort_unstable_by_key(|(number, _)| *number);
    Ok(files)
}
