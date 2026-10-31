//! Binary snapshot of the logical index.

use crate::index::IndexKey;
use crate::segment::Position;
use crate::vfs::{IoFile, OpenOptions, Vfs, VfsI};
use std::io::Result;
use std::path::Path;

const ENTRY_SIZE: usize = 28;

pub(super) type SnapshotEntry = (IndexKey, Position);

pub(super) fn write(vfs: &VfsI, path: &Path, entries: &[SnapshotEntry]) -> Result<()> {
    let tmp = path.with_extension("tmp");
    let _ = vfs.remove(&tmp);
    let file = vfs.open(&tmp, OpenOptions::CreateNew)?;

    let mut buf = Vec::with_capacity(8 + entries.len() * ENTRY_SIZE);
    buf.extend_from_slice(&(entries.len() as u64).to_le_bytes());
    for ((stream_id, offset), position) in entries {
        buf.extend_from_slice(&stream_id.to_le_bytes());
        buf.extend_from_slice(&offset.to_le_bytes());
        buf.extend_from_slice(&position.segment.to_le_bytes());
        buf.extend_from_slice(&position.offset.to_le_bytes());
        buf.extend_from_slice(&position.len.to_le_bytes());
    }
    file.append(&buf)?;
    file.sync()?;
    drop(file);

    let _ = vfs.remove(path);
    vfs.rename(&tmp, path)?;
    vfs.sync(path.parent().unwrap_or_else(|| Path::new(".")))
}

pub(super) fn read(vfs: &VfsI, path: &Path) -> Result<Vec<SnapshotEntry>> {
    match vfs.open(path, OpenOptions::Existing) {
        Ok(file) => {
            let size = file.size()?;
            let bytes = file.read_at(0, size as usize)?;
            Ok(decode(&bytes))
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(Vec::new()),
        Err(error) => Err(error),
    }
}

fn decode(bytes: &[u8]) -> Vec<SnapshotEntry> {
    if bytes.len() < 8 {
        return Vec::new();
    }
    let count = u64::from_le_bytes(bytes[0..8].try_into().unwrap()) as usize;
    let mut entries = Vec::with_capacity(count.min(bytes.len() / ENTRY_SIZE));
    let mut position = 8usize;
    for _ in 0..count {
        if position + ENTRY_SIZE > bytes.len() {
            break;
        }
        let stream_id = i64::from_le_bytes(bytes[position..position + 8].try_into().unwrap());
        let offset = u64::from_le_bytes(bytes[position + 8..position + 16].try_into().unwrap());
        let segment = u32::from_le_bytes(bytes[position + 16..position + 20].try_into().unwrap());
        let segment_offset =
            u32::from_le_bytes(bytes[position + 20..position + 24].try_into().unwrap());
        let len = u32::from_le_bytes(bytes[position + 24..position + 28].try_into().unwrap());
        entries.push((
            (stream_id, offset),
            Position {
                segment,
                offset: segment_offset,
                len,
            },
        ));
        position += ENTRY_SIZE;
    }
    entries
}
