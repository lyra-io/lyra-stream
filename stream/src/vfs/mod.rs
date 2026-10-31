//! Virtual filesystem interfaces used by stream storage and WAL segments.

mod direct;
mod failpoint;
mod memory;
mod standard;

pub use direct::{DirectFile, DirectVfs};
pub use failpoint::{Failpoint, FailpointFile, FailpointHandle, FailpointVfs};
pub use memory::{MemoryFile, MemoryVfs};
pub use standard::{StandardFile, StandardVfs};

use bytes::Bytes;
use std::fmt::Debug;
use std::io::Result;
use std::path::{Path, PathBuf};

/// An opened file supplied by one of the supported virtual filesystems.
#[derive(Debug)]
pub enum VfsFile {
    Memory(MemoryFile),
    Standard(StandardFile),
    Direct(DirectFile),
    Failpoint(FailpointFile),
}

/// One of the supported virtual filesystem implementations.
#[derive(Debug, Clone)]
pub enum VfsI {
    Memory(MemoryVfs),
    Standard(StandardVfs),
    Direct(DirectVfs),
    Failpoint(FailpointVfs),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpenOptions {
    Existing,
    CreateNew,
}

/// Positioned reads and single-writer appends required by segments.
///
/// Calls to [`IoFile::append`] and [`IoFile::truncate`] must be serialized by
/// the caller. Reads and synchronization may run concurrently with the writer.
pub trait IoFile: Debug + Send + Sync {
    fn path(&self) -> &Path;

    fn size(&self) -> Result<u64>;

    /// Reads exactly `length` bytes starting at `position`.
    fn read_at(&self, position: u64, length: usize) -> Result<Bytes>;

    /// Appends all bytes and returns their starting file position.
    fn append(&self, bytes: &[u8]) -> Result<u64>;

    /// Removes bytes after `size` when rebuilding a segment tail.
    fn truncate(&self, size: u64) -> Result<()>;

    fn sync(&self) -> Result<()>;

    /// Advises the operating system that cached bytes before `end` are no
    /// longer needed. Implementations without a page cache may do nothing.
    fn discard_cache(&self, end: u64);
}

/// Filesystem namespace and file-opening operations needed by segments.
pub trait Vfs: Debug + Send + Sync {
    /// Returns the required file-offset and I/O-length alignment, if any.
    fn alignment(&self) -> Option<u64>;

    fn create_dir(&self, path: &Path) -> Result<()>;

    fn open(&self, path: &Path, options: OpenOptions) -> Result<VfsFile>;

    fn list(&self, dir: &Path) -> Result<Vec<PathBuf>>;

    fn remove(&self, path: &Path) -> Result<()>;

    /// Moves `from` to `to`, replacing any existing destination file.
    fn rename(&self, _from: &Path, _to: &Path) -> Result<()> {
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "rename is not supported by this virtual filesystem",
        ))
    }

    fn sync(&self, dir: &Path) -> Result<()>;
}

impl Vfs for VfsI {
    fn alignment(&self) -> Option<u64> {
        match self {
            Self::Memory(vfs) => vfs.alignment(),
            Self::Standard(vfs) => vfs.alignment(),
            Self::Direct(vfs) => vfs.alignment(),
            Self::Failpoint(vfs) => vfs.alignment(),
        }
    }

    fn create_dir(&self, path: &Path) -> Result<()> {
        match self {
            Self::Memory(vfs) => vfs.create_dir(path),
            Self::Standard(vfs) => vfs.create_dir(path),
            Self::Direct(vfs) => vfs.create_dir(path),
            Self::Failpoint(vfs) => vfs.create_dir(path),
        }
    }

    fn open(&self, path: &Path, options: OpenOptions) -> Result<VfsFile> {
        match self {
            Self::Memory(vfs) => vfs.open(path, options),
            Self::Standard(vfs) => vfs.open(path, options),
            Self::Direct(vfs) => vfs.open(path, options),
            Self::Failpoint(vfs) => vfs.open(path, options),
        }
    }

    fn list(&self, dir: &Path) -> Result<Vec<PathBuf>> {
        match self {
            Self::Memory(vfs) => vfs.list(dir),
            Self::Standard(vfs) => vfs.list(dir),
            Self::Direct(vfs) => vfs.list(dir),
            Self::Failpoint(vfs) => vfs.list(dir),
        }
    }

    fn remove(&self, path: &Path) -> Result<()> {
        match self {
            Self::Memory(vfs) => vfs.remove(path),
            Self::Standard(vfs) => vfs.remove(path),
            Self::Direct(vfs) => vfs.remove(path),
            Self::Failpoint(vfs) => vfs.remove(path),
        }
    }

    fn rename(&self, from: &Path, to: &Path) -> Result<()> {
        match self {
            Self::Memory(vfs) => vfs.rename(from, to),
            Self::Standard(vfs) => vfs.rename(from, to),
            Self::Direct(vfs) => vfs.rename(from, to),
            Self::Failpoint(vfs) => vfs.rename(from, to),
        }
    }

    fn sync(&self, dir: &Path) -> Result<()> {
        match self {
            Self::Memory(vfs) => vfs.sync(dir),
            Self::Standard(vfs) => vfs.sync(dir),
            Self::Direct(vfs) => vfs.sync(dir),
            Self::Failpoint(vfs) => vfs.sync(dir),
        }
    }
}

impl IoFile for VfsFile {
    fn path(&self) -> &Path {
        match self {
            Self::Memory(file) => file.path(),
            Self::Standard(file) => file.path(),
            Self::Direct(file) => file.path(),
            Self::Failpoint(file) => file.path(),
        }
    }

    fn size(&self) -> Result<u64> {
        match self {
            Self::Memory(file) => file.size(),
            Self::Standard(file) => file.size(),
            Self::Direct(file) => file.size(),
            Self::Failpoint(file) => file.size(),
        }
    }

    fn read_at(&self, position: u64, length: usize) -> Result<Bytes> {
        match self {
            Self::Memory(file) => file.read_at(position, length),
            Self::Standard(file) => file.read_at(position, length),
            Self::Direct(file) => file.read_at(position, length),
            Self::Failpoint(file) => file.read_at(position, length),
        }
    }

    fn append(&self, bytes: &[u8]) -> Result<u64> {
        match self {
            Self::Memory(file) => file.append(bytes),
            Self::Standard(file) => file.append(bytes),
            Self::Direct(file) => file.append(bytes),
            Self::Failpoint(file) => file.append(bytes),
        }
    }

    fn truncate(&self, size: u64) -> Result<()> {
        match self {
            Self::Memory(file) => file.truncate(size),
            Self::Standard(file) => file.truncate(size),
            Self::Direct(file) => file.truncate(size),
            Self::Failpoint(file) => file.truncate(size),
        }
    }

    fn sync(&self) -> Result<()> {
        match self {
            Self::Memory(file) => file.sync(),
            Self::Standard(file) => file.sync(),
            Self::Direct(file) => file.sync(),
            Self::Failpoint(file) => file.sync(),
        }
    }

    fn discard_cache(&self, end: u64) {
        match self {
            Self::Memory(file) => file.discard_cache(end),
            Self::Standard(file) => file.discard_cache(end),
            Self::Direct(file) => file.discard_cache(end),
            Self::Failpoint(file) => file.discard_cache(end),
        }
    }
}
