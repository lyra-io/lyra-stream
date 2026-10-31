//! Durable commit offset owned by the WAL.

use crate::vfs::{IoFile, OpenOptions, Vfs, VfsI};
use std::io::Result;
use std::path::Path;

const CHECKPOINT_FILE: &str = "checkpoint";

pub(crate) fn path(dir: &Path) -> std::path::PathBuf {
    dir.join(CHECKPOINT_FILE)
}

pub(crate) fn read(vfs: &VfsI, dir: &Path) -> Result<u64> {
    match vfs.open(&path(dir), OpenOptions::Existing) {
        Ok(file) => {
            let size = file.size()?;
            let bytes = file.read_at(0, size as usize)?;
            let contents = String::from_utf8_lossy(&bytes);
            Ok(contents.trim().parse().unwrap_or(0))
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(0),
        Err(error) => Err(error),
    }
}

pub(crate) fn write(vfs: &VfsI, dir: &Path, commit_offset: u64) -> Result<()> {
    let checkpoint = path(dir);
    let tmp = dir.join(format!("{CHECKPOINT_FILE}.tmp"));
    let _ = vfs.remove(&tmp);

    let file = vfs.open(&tmp, OpenOptions::CreateNew)?;
    file.append(format!("{commit_offset}\n").as_bytes())?;
    file.sync()?;
    drop(file);

    let _ = vfs.remove(&checkpoint);
    vfs.rename(&tmp, &checkpoint)?;
    vfs.sync(dir)
}
