//! Tiered stream storage.
//!
//! Level 0 is the mutable memstream write cache; level 1 is the immutable
//! memstream; level 2 is a composite on-disk file. Higher levels (per-stream
//! files, remote) are added incrementally. The index stores only a
//! [`Location`] for each primary offset, and compaction rewrites locations in
//! one atomic batch per tier move.

use crate::vfs::{IoFile, OpenOptions, StandardVfs, Vfs, VfsI};
use bytes::Bytes;
use dashmap::DashMap;
use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::RwLock;
use std::sync::atomic::{AtomicU64, Ordering};

pub const LEVEL_MEMSTREAM: u8 = 0;
pub const LEVEL_IMMUTABLE: u8 = 1;
pub const LEVEL_COMPOSITE: u8 = 2;

/// A switched-out L0 memstream handed to the compaction chain.
pub type Memstream = DashMap<(i64, i64), Bytes>;

/// Physical location of a record inside the tiered store.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Location {
    pub level: u8,
    pub file_id: u64,
    pub position: u64,
    pub length: u32,
}

impl Location {
    pub const ENCODED_LEN: usize = 21;

    pub fn memstream(length: u32) -> Self {
        Self {
            level: LEVEL_MEMSTREAM,
            file_id: 0,
            position: 0,
            length,
        }
    }

    pub fn immutable(length: u32) -> Self {
        Self {
            level: LEVEL_IMMUTABLE,
            file_id: 0,
            position: 0,
            length,
        }
    }

    pub fn composite(file_id: u64, position: u64, length: u32) -> Self {
        Self {
            level: LEVEL_COMPOSITE,
            file_id,
            position,
            length,
        }
    }

    pub fn encode(self) -> [u8; Self::ENCODED_LEN] {
        let mut bytes = [0u8; Self::ENCODED_LEN];
        bytes[0] = self.level;
        bytes[1..9].copy_from_slice(&self.file_id.to_be_bytes());
        bytes[9..17].copy_from_slice(&self.position.to_be_bytes());
        bytes[17..21].copy_from_slice(&self.length.to_be_bytes());
        bytes
    }

    pub fn decode(bytes: &[u8]) -> Option<Self> {
        if bytes.len() != Self::ENCODED_LEN {
            return None;
        }
        Some(Self {
            level: bytes[0],
            file_id: u64::from_be_bytes(bytes[1..9].try_into().ok()?),
            position: u64::from_be_bytes(bytes[9..17].try_into().ok()?),
            length: u32::from_be_bytes(bytes[17..21].try_into().ok()?),
        })
    }
}

/// On-disk composite files (L2), one file holding records from many streams.
pub struct FileStore {
    dir: PathBuf,
    vfs: VfsI,
    next_file_id: AtomicU64,
}

impl FileStore {
    pub fn new(dir: PathBuf) -> Self {
        let vfs = VfsI::Standard(StandardVfs);
        let _ = vfs.create_dir(&dir);
        Self {
            dir,
            vfs,
            next_file_id: AtomicU64::new(1),
        }
    }

    /// Writes records sorted by `(stream_id, offset)` into a new composite
    /// file and returns each record's L2 location.
    pub fn write_composite(
        &self,
        records: &[(i64, i64, Bytes)],
    ) -> Result<Vec<(i64, i64, Location)>, std::io::Error> {
        let file_id = self.next_file_id.fetch_add(1, Ordering::SeqCst);
        let path = self.dir.join(format!("{file_id:010}.l2"));
        let file = self.vfs.open(&path, OpenOptions::CreateNew)?;
        let mut out = Vec::with_capacity(records.len());
        for (stream_id, offset, payload) in records {
            let mut header = [0u8; 20];
            header[..8].copy_from_slice(&stream_id.to_be_bytes());
            header[8..16].copy_from_slice(&offset.to_be_bytes());
            header[16..20].copy_from_slice(&(payload.len() as u32).to_be_bytes());
            file.append(&header)?;
            let position = file.append(payload)?;
            out.push((
                *stream_id,
                *offset,
                Location::composite(file_id, position, payload.len() as u32),
            ));
        }
        file.sync()?;
        Ok(out)
    }

    pub fn read(&self, location: Location) -> Option<Bytes> {
        if location.level != LEVEL_COMPOSITE {
            return None;
        }
        let path = self.dir.join(format!("{:010}.l2", location.file_id));
        let file = self.vfs.open(&path, OpenOptions::Existing).ok()?;
        file.read_at(location.position, location.length as usize).ok()
    }
}

/// In-memory tiers plus a file store, owned by the stream node.
pub struct Storage {
    l0: RwLock<Arc<DashMap<(i64, i64), Bytes>>>,
    backup: RwLock<VecDeque<Arc<DashMap<(i64, i64), Bytes>>>>,
    l1: DashMap<(i64, i64), Bytes>,
    l0_bytes: AtomicU64,
    l0_threshold: u64,
    l1_bytes: AtomicU64,
    l1_threshold: u64,
    files: Arc<FileStore>,
}

impl Storage {
    pub fn new(l0_threshold: u64, l1_threshold: u64, files: Arc<FileStore>) -> Self {
        Self {
            l0: RwLock::new(Arc::new(DashMap::new())),
            backup: RwLock::new(VecDeque::new()),
            l1: DashMap::new(),
            l0_bytes: AtomicU64::new(0),
            l0_threshold,
            l1_bytes: AtomicU64::new(0),
            l1_threshold,
            files,
        }
    }

    /// Inserts into L0. When L0 crosses its threshold, swaps in a fresh
    /// memstream and queues the old one as a readable backup for compaction.
    /// Returns `true` when a switch happened.
    pub fn put(&self, stream_id: i64, offset: i64, payload: Bytes) -> bool {
        let len = payload.len() as u64;
        let mut guard = self.l0.write().expect("l0 lock poisoned");
        guard.insert((stream_id, offset), payload);
        let bytes = self.l0_bytes.fetch_add(len, Ordering::Relaxed) + len;
        if bytes >= self.l0_threshold {
            let old = std::mem::replace(&mut *guard, Arc::new(DashMap::new()));
            self.l0_bytes.store(0, Ordering::Relaxed);
            self.backup.write().expect("backup lock poisoned").push_back(old);
            true
        } else {
            false
        }
    }

    /// Inserts into L0 without swapping, used when rebuilding from the WAL.
    pub fn replay_put(&self, stream_id: i64, offset: i64, payload: Bytes) {
        let len = payload.len() as u64;
        let l0 = self.l0.read().expect("l0 lock poisoned").clone();
        l0.insert((stream_id, offset), payload);
        self.l0_bytes.fetch_add(len, Ordering::Relaxed);
    }

    /// Moves the oldest switched backup into the immutable L1 tier.
    pub fn promote_backup(&self) -> Vec<(i64, i64, u32)> {
        let mut moved = Vec::new();
        let old = {
            let backups = self.backup.read().expect("backup lock poisoned");
            match backups.front() {
                Some(old) => old.clone(),
                None => return moved,
            }
        };
        for entry in old.iter() {
            let (key, payload) = entry.pair();
            let (stream_id, offset) = *key;
            let length = payload.len() as u32;
            self.l1.insert((stream_id, offset), payload.clone());
            self.l1_bytes.fetch_add(length as u64, Ordering::Relaxed);
            moved.push((stream_id, offset, length));
        }
        // Only pop after every record is visible in L1, so reads fall back to
        // the backup during the move instead of missing.
        self.backup.write().expect("backup lock poisoned").pop_front();
        moved
    }

    pub fn l1_needs_compaction(&self) -> bool {
        self.l1_bytes.load(Ordering::Relaxed) >= self.l1_threshold
    }

    /// Snapshots L1 into a composite L2 file and returns the new locations.
    /// The L1 entries are kept until [`Storage::discard_l1`] is called after
    /// the index has been switched, so reads never miss during the move.
    pub fn compact_l1(&self) -> Vec<(i64, i64, Location)> {
        let mut records: Vec<(i64, i64, Bytes)> = self
            .l1
            .iter()
            .map(|entry| {
                let (key, payload) = entry.pair();
                ((*key).0, (*key).1, payload.clone())
            })
            .collect();
        records.sort_by(|a, b| (a.0, a.1).cmp(&(b.0, b.1)));
        self.files.write_composite(&records).unwrap_or_default()
    }

    /// Removes compacted records from L1 after their index now points at L2.
    pub fn discard_l1(&self, locations: &[(i64, i64, Location)]) {
        for (stream_id, offset, location) in locations {
            self.l1.remove(&(*stream_id, *offset));
            self.l1_bytes
                .fetch_sub(location.length as u64, Ordering::Relaxed);
        }
    }

    pub fn get(&self, location: Location, stream_id: i64, offset: i64) -> Option<Bytes> {
        match location.level {
            LEVEL_MEMSTREAM => {
                let l0 = self.l0.read().expect("l0 lock poisoned").clone();
                if let Some(value) = l0.get(&(stream_id, offset)) {
                    return Some(value.clone());
                }
                let backups = self.backup.read().expect("backup lock poisoned");
                for backup in backups.iter() {
                    if let Some(value) = backup.get(&(stream_id, offset)) {
                        return Some(value.clone());
                    }
                }
                self.l1.get(&(stream_id, offset)).map(|value| value.clone())
            }
            LEVEL_IMMUTABLE => self.l1.get(&(stream_id, offset)).map(|value| value.clone()),
            LEVEL_COMPOSITE => self.files.read(location),
            _ => None,
        }
    }
}
