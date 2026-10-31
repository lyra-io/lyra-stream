//! Fixed-size raw-buffer segment format.
//!
//! A segment is a fixed-size, append-only file named `{segment_id:010}.seg`.
//! The file contains raw buffers only:
//!
//! ```text
//! ┌──────────────────────────────────────────────┐
//! │ buffer bytes...                              │
//! │ (no header, no length prefix, no checksum)   │
//! └──────────────────────────────────────────────┘
//! ```
//!
//! Callers store raw buffers and remember `(segment_id, offset, len)`
//! positions. Record framing lives in [`super::record`] or
//! [`super::recordio`].

use crate::vfs::{Vfs, VfsI};
use std::io::Result;
use std::path::{Path, PathBuf};

pub const SEGMENT_EXTENSION: &str = "seg";
pub const SEGMENT_NAME_LEN: usize = 10;

pub fn make_segment_path(dir: &Path, segment_id: u64) -> PathBuf {
    dir.join(format!(
        "{segment_id:0SEGMENT_NAME_LEN$}.{SEGMENT_EXTENSION}"
    ))
}

pub fn list_segments(vfs: &VfsI, dir: &Path) -> Result<Vec<(u64, PathBuf)>> {
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
        if stem.len() != SEGMENT_NAME_LEN || !stem.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "segment filename must contain exactly {SEGMENT_NAME_LEN} digits: {}",
                    path.display()
                ),
            ));
        }
        let segment_id = stem.parse::<u64>().map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("segment filename is not numeric: {}", path.display()),
            )
        })?;
        files.push((segment_id, path));
    }
    files.sort_unstable_by_key(|(segment_id, _)| *segment_id);
    Ok(files)
}
