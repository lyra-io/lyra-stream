//! A VFS decorator that injects I/O failures for fault-injection tests.

use super::{IoFile, OpenOptions, Vfs, VfsFile};
use bytes::Bytes;
use std::collections::HashSet;
use std::io::{Error, Result};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

/// Names of the operations that can be failed by [`FailpointVfs`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Failpoint {
    Open,
    Read,
    Append,
    Truncate,
    Sync,
    Rename,
}

impl Failpoint {
    fn as_str(self) -> &'static str {
        match self {
            Self::Open => "open",
            Self::Read => "read",
            Self::Append => "append",
            Self::Truncate => "truncate",
            Self::Sync => "sync",
            Self::Rename => "rename",
        }
    }
}

#[derive(Debug, Default)]
struct FailpointState {
    enabled: Mutex<HashSet<Failpoint>>,
}

impl FailpointState {
    fn set(&self, point: Failpoint, enabled: bool) {
        let mut enabled_points = self.enabled.lock().expect("failpoint lock poisoned");
        if enabled {
            enabled_points.insert(point);
        } else {
            enabled_points.remove(&point);
        }
    }

    fn fail(&self, point: Failpoint, path: &Path) -> Result<()> {
        if self
            .enabled
            .lock()
            .expect("failpoint lock poisoned")
            .contains(&point)
        {
            return Err(Error::other(format!(
                "injected {} failure for {}",
                point.as_str(),
                path.display()
            )));
        }
        Ok(())
    }
}

/// Controls failpoints for one [`FailpointVfs`].
#[derive(Debug, Clone, Default)]
pub struct FailpointHandle {
    state: Arc<FailpointState>,
}

impl FailpointHandle {
    pub fn enable(&self, point: Failpoint) {
        self.state.set(point, true);
    }

    pub fn disable(&self, point: Failpoint) {
        self.state.set(point, false);
    }
}

/// Wraps another VFS and injects failures where enabled.
#[derive(Debug, Clone)]
pub struct FailpointVfs {
    inner: Box<super::VfsI>,
    state: Arc<FailpointState>,
}

impl FailpointVfs {
    pub fn new(inner: super::VfsI) -> (Self, FailpointHandle) {
        let state = Arc::new(FailpointState::default());
        (
            Self {
                inner: Box::new(inner),
                state: Arc::clone(&state),
            },
            FailpointHandle { state },
        )
    }
}

impl Vfs for FailpointVfs {
    fn alignment(&self) -> Option<u64> {
        self.inner.alignment()
    }

    fn create_dir(&self, path: &Path) -> Result<()> {
        self.inner.create_dir(path)
    }

    fn open(&self, path: &Path, options: OpenOptions) -> Result<VfsFile> {
        self.state.fail(Failpoint::Open, path)?;
        let file = self.inner.open(path, options)?;
        Ok(VfsFile::Failpoint(FailpointFile {
            path: path.to_path_buf(),
            inner: Box::new(file),
            state: Arc::clone(&self.state),
        }))
    }

    fn list(&self, dir: &Path) -> Result<Vec<PathBuf>> {
        self.inner.list(dir)
    }

    fn remove(&self, path: &Path) -> Result<()> {
        self.inner.remove(path)
    }

    fn rename(&self, from: &Path, to: &Path) -> Result<()> {
        self.state.fail(Failpoint::Rename, from)?;
        self.inner.rename(from, to)
    }

    fn sync(&self, dir: &Path) -> Result<()> {
        self.inner.sync(dir)
    }
}

#[derive(Debug)]
pub struct FailpointFile {
    path: PathBuf,
    inner: Box<VfsFile>,
    state: Arc<FailpointState>,
}

impl IoFile for FailpointFile {
    fn path(&self) -> &Path {
        &self.path
    }

    fn size(&self) -> Result<u64> {
        self.inner.size()
    }

    fn read_at(&self, position: u64, length: usize) -> Result<Bytes> {
        self.state.fail(Failpoint::Read, &self.path)?;
        self.inner.read_at(position, length)
    }

    fn append(&self, bytes: &[u8]) -> Result<u64> {
        self.state.fail(Failpoint::Append, &self.path)?;
        self.inner.append(bytes)
    }

    fn truncate(&self, size: u64) -> Result<()> {
        self.state.fail(Failpoint::Truncate, &self.path)?;
        self.inner.truncate(size)
    }

    fn sync(&self) -> Result<()> {
        self.state.fail(Failpoint::Sync, &self.path)?;
        self.inner.sync()
    }

    fn discard_cache(&self, end: u64) {
        self.inner.discard_cache(end);
    }
}
