//! Stream storage: a shared write-ahead log for durability and an mmap'd data
//! store served by the page cache.
//!
//! ```text
//! append ──► WAL (group-committed fdatasync)          [durable, read only on recovery]
//!        ──► mmap'd segment (page cache)              [read path]
//!        ──► in-memory index (stream_id, offset) -> position
//! ```
//!
//! A checkpoint `msync`s the data store, snapshots the index, persists the
//! commit offset and append position, then trims the WAL up to the commit
//! offset. Recovery trusts the data store up to the checkpoint and replays only
//! the WAL tail.

use bytes::Bytes;
use memmap2::MmapMut;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::mpsc::{Receiver, Sender, TryRecvError};

const WAL_HEADER_SIZE: usize = 20;
const INDEX_ENTRY_SIZE: usize = 28;
const DEFAULT_SEGMENT_SIZE: usize = 64 * 1024 * 1024;

/// Durable checkpoint: where the WAL may be trimmed and where the data store
/// resumes appending.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct Checkpoint {
    commit_offset: u64,
    next_segment_id: u64,
    active_segment_id: u64,
    active_written: usize,
}

impl Checkpoint {
    fn read(path: &Path) -> std::io::Result<Self> {
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
            Err(error) => Err(error),
        }
    }

    fn write(&self, path: &Path) -> std::io::Result<()> {
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
        file.sync_data()
    }
}

/// Physical location of a record inside the mmap'd data store.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Position {
    segment: usize,
    offset: usize,
    len: u32,
}

struct WalRecord {
    stream_id: i64,
    offset: u64,
    payload: Vec<u8>,
}

enum WalCommand {
    Append {
        stream_id: i64,
        payload: Bytes,
        ack: Sender<u64>,
    },
    Trim {
        commit_offset: u64,
        ack: Sender<std::io::Result<()>>,
    },
}

/// The shared write-ahead log. A background thread batches appends and
/// resolves them after one `fdatasync` for the batch. Trims are executed by the
/// same thread so the append handle stays valid across file rotation.
pub struct Wal {
    tx: Option<Sender<WalCommand>>,
    handle: Option<std::thread::JoinHandle<()>>,
}

impl Wal {
    pub fn open(path: &Path, start_sequence: u64) -> std::io::Result<Self> {
        let path = path.to_path_buf();
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&path)?;
        let (tx, rx) = std::sync::mpsc::channel();
        let handle = std::thread::Builder::new()
            .name("lyra-wal-writer".into())
            .spawn(move || wal_writer_loop(path, file, rx, start_sequence))?;
        Ok(Self {
            tx: Some(tx),
            handle: Some(handle),
        })
    }

    /// Appends a record and returns its assigned offset once durable.
    pub fn append(&self, stream_id: i64, payload: Bytes) -> u64 {
        let (ack, rx) = std::sync::mpsc::channel();
        self.tx
            .as_ref()
            .expect("wal closed")
            .send(WalCommand::Append {
                stream_id,
                payload,
                ack,
            })
            .expect("wal writer closed");
        rx.recv().expect("wal writer closed")
    }

    /// Trims records with offset strictly below `commit_offset`.
    pub fn trim(&self, commit_offset: u64) -> std::io::Result<()> {
        let (ack, rx) = std::sync::mpsc::channel();
        self.tx
            .as_ref()
            .expect("wal closed")
            .send(WalCommand::Trim { commit_offset, ack })
            .expect("wal writer closed");
        rx.recv().expect("wal writer closed")
    }
}

impl Drop for Wal {
    fn drop(&mut self) {
        self.tx.take(); // disconnect the channel so the writer thread exits
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

fn wal_writer_loop(path: PathBuf, mut file: File, rx: Receiver<WalCommand>, mut sequence: u64) {
    let mut batch: Vec<(Sender<u64>, u64)> = Vec::new();
    let mut pending: Option<WalCommand> = None;

    loop {
        let command = match pending.take() {
            Some(command) => command,
            None => match rx.recv() {
                Ok(command) => command,
                Err(_) => break,
            },
        };

        match command {
            WalCommand::Append {
                stream_id,
                payload,
                ack,
            } => {
                sequence += 1;
                let mut record = Vec::with_capacity(WAL_HEADER_SIZE + payload.len());
                append_wal_record(&mut record, stream_id, sequence, &payload);
                if let Err(error) = file.write_all(&record) {
                    tracing::error!(%error, "WAL write failed");
                    let _ = ack.send(0);
                    continue;
                }
                batch.push((ack, sequence));
            }
            WalCommand::Trim { commit_offset, ack } => {
                let result = trim_wal_file(&path, &mut file, commit_offset);
                let _ = ack.send(result);
                continue;
            }
        }

        // Drain whatever else is queued before syncing (group commit). Trims
        // are stashed and processed after the batch is durable.
        loop {
            match rx.try_recv() {
                Ok(WalCommand::Append {
                    stream_id,
                    payload,
                    ack,
                }) => {
                    sequence += 1;
                    let mut record = Vec::with_capacity(WAL_HEADER_SIZE + payload.len());
                    append_wal_record(&mut record, stream_id, sequence, &payload);
                    if let Err(error) = file.write_all(&record) {
                        tracing::error!(%error, "WAL write failed");
                        let _ = ack.send(0);
                        continue;
                    }
                    batch.push((ack, sequence));
                }
                Ok(command @ WalCommand::Trim { .. }) => {
                    pending = Some(command);
                    break;
                }
                Err(TryRecvError::Empty) | Err(TryRecvError::Disconnected) => break,
            }
        }

        if !batch.is_empty() && let Err(error) = file.sync_data() {
            tracing::error!(%error, "WAL fsync failed");
        }
        for (ack, offset) in batch.drain(..) {
            let _ = ack.send(offset);
        }
    }
}

fn trim_wal_file(path: &Path, file: &mut File, commit_offset: u64) -> std::io::Result<()> {
    let records = read_wal_records(file)?;
    let mut tail = Vec::new();
    for record in records {
        if record.offset >= commit_offset {
            append_wal_record(&mut tail, record.stream_id, record.offset, &record.payload);
        }
    }

    let tmp = path.with_extension("tmp");
    let mut out = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&tmp)?;
    out.write_all(&tail)?;
    out.sync_data()?;
    std::fs::rename(&tmp, path)?;

    // Reopen the append handle so subsequent writes land in the rotated file.
    *file = OpenOptions::new()
        .create(true)
        .read(true)
        .append(true)
        .open(path)?;
    Ok(())
}

fn append_wal_record(out: &mut Vec<u8>, stream_id: i64, offset: u64, payload: &[u8]) {
    out.extend_from_slice(&stream_id.to_le_bytes());
    out.extend_from_slice(&offset.to_le_bytes());
    out.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    out.extend_from_slice(payload);
}

fn read_wal_records(file: &mut File) -> std::io::Result<Vec<WalRecord>> {
    file.seek(SeekFrom::Start(0))?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)?;
    let mut records = Vec::new();
    let mut position = 0usize;
    while position + WAL_HEADER_SIZE <= bytes.len() {
        let stream_id = i64::from_le_bytes(bytes[position..position + 8].try_into().unwrap());
        let offset = u64::from_le_bytes(bytes[position + 8..position + 16].try_into().unwrap());
        let len =
            u32::from_le_bytes(bytes[position + 16..position + 20].try_into().unwrap()) as usize;
        if position + WAL_HEADER_SIZE + len > bytes.len() {
            break; // torn tail from an unclean shutdown
        }
        let payload = bytes[position + WAL_HEADER_SIZE..position + WAL_HEADER_SIZE + len].to_vec();
        records.push(WalRecord {
            stream_id,
            offset,
            payload,
        });
        position += WAL_HEADER_SIZE + len;
    }
    Ok(records)
}

/// mmap'd data segment, served by the page cache.
struct Segment {
    map: MmapMut,
    written: usize,
    capacity: usize,
}

impl Segment {
    fn open(dir: &Path, id: u64, capacity: usize, written: usize) -> std::io::Result<Self> {
        let path = dir.join(format!("{id:010}.seg"));
        let file = OpenOptions::new().read(true).write(true).open(&path)?;
        if file.metadata()?.len() < capacity as u64 {
            file.set_len(capacity as u64)?;
        }
        // SAFETY: the file is owned by this process and never truncated while
        // mapped. Fixed-size segment files avoid SIGBUS from external truncation.
        let map = unsafe { MmapMut::map_mut(&file)? };
        Ok(Self {
            map,
            written,
            capacity,
        })
    }

    fn create(dir: &Path, id: u64, capacity: usize) -> std::io::Result<Self> {
        let path = dir.join(format!("{id:010}.seg"));
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(true)
            .open(path)?;
        file.set_len(capacity as u64)?;
        // SAFETY: the file is owned by this process and never truncated while
        // mapped.
        let map = unsafe { MmapMut::map_mut(&file)? };
        Ok(Self {
            map,
            written: 0,
            capacity,
        })
    }

    fn remaining(&self) -> usize {
        self.capacity - self.written
    }

    fn read(&self, offset: usize, len: u32) -> &[u8] {
        &self.map[offset..offset + len as usize]
    }

    fn sync(&self) -> std::io::Result<()> {
        self.map.flush()
    }
}

/// The storage engine: shared WAL + mmap'd data store + in-memory index.
pub struct Storage {
    dir: PathBuf,
    wal: Wal,
    segments: Vec<Segment>,
    active: usize,
    segment_size: usize,
    next_segment_id: u64,
    index: HashMap<(i64, u64), Position>,
    checkpoint_path: PathBuf,
    index_path: PathBuf,
    checkpoint_offset: u64,
    highest_offset: u64,
}

impl Storage {
    pub fn open(dir: impl AsRef<Path>, segment_size: Option<usize>) -> std::io::Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&dir)?;
        let segment_size = segment_size.unwrap_or(DEFAULT_SEGMENT_SIZE);
        let checkpoint_path = dir.join("checkpoint");
        let index_path = dir.join("index.snap");
        let checkpoint = Checkpoint::read(&checkpoint_path)?;

        let wal_path = dir.join("wal.log");
        let wal_records = match File::open(&wal_path) {
            Ok(mut file) => read_wal_records(&mut file)?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Vec::new(),
            Err(error) => return Err(error),
        };
        let wal_max = wal_records.iter().map(|r| r.offset).max().unwrap_or(0);
        let start_sequence = wal_max.max(checkpoint.commit_offset.saturating_sub(1));
        let wal = Wal::open(&wal_path, start_sequence)?;

        // Rebuild the segments up to the checkpointed active segment.
        let mut segments = Vec::new();
        for id in 0..checkpoint.next_segment_id {
            let written = if id == checkpoint.active_segment_id {
                checkpoint.active_written
            } else {
                segment_size
            };
            segments.push(Segment::open(&dir, id, segment_size, written)?);
        }
        let active = if segments.is_empty() {
            segments.push(Segment::create(&dir, 0, segment_size)?);
            0
        } else {
            let active = checkpoint.active_segment_id as usize;
            if active >= segments.len() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!(
                        "checkpoint active segment {active} exceeds segment count {}",
                        segments.len()
                    ),
                ));
            }
            active
        };
        let next_segment_id = segments.len() as u64;

        let mut storage = Self {
            dir,
            wal,
            segments,
            active,
            segment_size,
            next_segment_id,
            index: HashMap::new(),
            checkpoint_path,
            index_path,
            checkpoint_offset: checkpoint.commit_offset,
            highest_offset: start_sequence,
        };

        // Load the index that was durable at the checkpoint, then overlay the
        // WAL tail.
        storage.index = read_index_snapshot(&storage.index_path, &checkpoint, &storage.segments)?;
        for record in wal_records {
            if record.offset < storage.checkpoint_offset {
                continue;
            }
            let position = storage.append_to_segment(&record.payload);
            storage
                .index
                .insert((record.stream_id, record.offset), position);
            storage.highest_offset = storage.highest_offset.max(record.offset);
        }

        Ok(storage)
    }

    fn append_to_segment(&mut self, payload: &[u8]) -> Position {
        if self.segments[self.active].remaining() < payload.len() {
            let segment = Segment::create(&self.dir, self.next_segment_id, self.segment_size)
                .expect("create segment");
            self.segments.push(segment);
            self.active = self.segments.len() - 1;
            self.next_segment_id += 1;
        }
        let segment = self.active;
        let offset = self.segments[segment].written;
        let len = payload.len() as u32;
        self.segments[segment].map[offset..offset + payload.len()].copy_from_slice(payload);
        self.segments[segment].written += payload.len();
        Position {
            segment,
            offset,
            len,
        }
    }

    pub fn append(&mut self, stream_id: i64, payload: Bytes) -> u64 {
        let offset = self.wal.append(stream_id, payload.clone());
        let position = self.append_to_segment(&payload);
        self.index.insert((stream_id, offset), position);
        self.highest_offset = self.highest_offset.max(offset);
        offset
    }

    pub fn read(&self, stream_id: i64, offset: u64) -> Option<&[u8]> {
        let position = self.index.get(&(stream_id, offset))?;
        Some(self.segments[position.segment].read(position.offset, position.len))
    }

    /// Makes the data store and index durable, then trims the WAL up to the
    /// current commit offset. Returns the persisted commit offset.
    pub fn checkpoint(&mut self) -> std::io::Result<u64> {
        for segment in &self.segments {
            segment.sync()?;
        }
        let checkpoint_offset = self.highest_offset + 1;
        write_index_snapshot(&self.index_path, &self.index)?;
        let checkpoint = Checkpoint {
            commit_offset: checkpoint_offset,
            next_segment_id: self.next_segment_id,
            active_segment_id: self.active as u64,
            active_written: self.segments[self.active].written,
        };
        checkpoint.write(&self.checkpoint_path)?;
        self.wal.trim(checkpoint_offset)?;
        self.checkpoint_offset = checkpoint_offset;
        Ok(checkpoint_offset)
    }

    pub fn commit_offset(&self) -> u64 {
        self.checkpoint_offset
    }
}

fn write_index_snapshot(path: &Path, index: &HashMap<(i64, u64), Position>) -> std::io::Result<()> {
    let tmp = path.with_extension("tmp");
    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&tmp)?;

    let mut entries: Vec<_> = index.iter().collect();
    entries.sort_unstable_by_key(|(left, _)| *left);

    let mut buf = Vec::with_capacity(8 + entries.len() * INDEX_ENTRY_SIZE);
    buf.extend_from_slice(&(entries.len() as u64).to_le_bytes());
    for ((stream_id, offset), position) in entries {
        buf.extend_from_slice(&stream_id.to_le_bytes());
        buf.extend_from_slice(&offset.to_le_bytes());
        buf.extend_from_slice(&(position.segment as u32).to_le_bytes());
        buf.extend_from_slice(&(position.offset as u32).to_le_bytes());
        buf.extend_from_slice(&position.len.to_le_bytes());
    }

    file.write_all(&buf)?;
    file.sync_data()?;
    std::fs::rename(&tmp, path)?;
    Ok(())
}

fn read_index_snapshot(
    path: &Path,
    checkpoint: &Checkpoint,
    segments: &[Segment],
) -> std::io::Result<HashMap<(i64, u64), Position>> {
    let mut index = HashMap::new();
    let mut file = match File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(index),
        Err(error) => return Err(error),
    };
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)?;
    if bytes.len() < 8 {
        return Ok(index);
    }
    let count = u64::from_le_bytes(bytes[0..8].try_into().unwrap()) as usize;
    let mut position = 8usize;

    for _ in 0..count {
        if position + INDEX_ENTRY_SIZE > bytes.len() {
            break;
        }
        let stream_id = i64::from_le_bytes(bytes[position..position + 8].try_into().unwrap());
        let offset = u64::from_le_bytes(bytes[position + 8..position + 16].try_into().unwrap());
        let segment = u32::from_le_bytes(bytes[position + 16..position + 20].try_into().unwrap());
        let segment_offset =
            u32::from_le_bytes(bytes[position + 20..position + 24].try_into().unwrap());
        let len = u32::from_le_bytes(bytes[position + 24..position + 28].try_into().unwrap());
        position += INDEX_ENTRY_SIZE;

        // Only trust entries that are consistent with the checkpoint. Anything
        // newer is reconstructed from the WAL tail.
        if offset >= checkpoint.commit_offset {
            continue;
        }
        let segment = segment as usize;
        if segment >= checkpoint.next_segment_id as usize || segment >= segments.len() {
            continue;
        }
        let segment_offset = segment_offset as usize;
        if segment_offset + len as usize > segments[segment].written {
            continue;
        }
        index.insert(
            (stream_id, offset),
            Position {
                segment,
                offset: segment_offset,
                len,
            },
        );
    }
    Ok(index)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn appends_reads_checkpoints_and_recovers() {
        let dir = tempfile::tempdir().unwrap();

        // Small segments force rotation.
        let mut storage = Storage::open(dir.path(), Some(32)).unwrap();
        let offsets: Vec<(i64, u64, Vec<u8>)> = (0..20u64)
            .map(|i| {
                let stream_id = (i % 2) as i64;
                let payload = format!("rec-{i}").into_bytes();
                let offset = storage.append(stream_id, Bytes::from(payload.clone()));
                (stream_id, offset, payload)
            })
            .collect();

        for (stream_id, offset, payload) in &offsets {
            assert_eq!(storage.read(*stream_id, *offset), Some(&payload[..]));
        }

        // Checkpoint and trim the WAL.
        let checkpoint = storage.checkpoint().unwrap();
        assert!(checkpoint > 0);
        assert_eq!(storage.commit_offset(), checkpoint);
        assert!(storage.read(0, offsets[0].1).is_some());

        // Append more after the checkpoint.
        let post = storage.append(7, Bytes::from_static(b"post-checkpoint"));

        drop(storage);

        // Reopen: pre-checkpoint data comes from the data store, the tail from
        // the WAL.
        let reopened = Storage::open(dir.path(), Some(32)).unwrap();
        for (stream_id, offset, payload) in &offsets {
            assert_eq!(
                reopened.read(*stream_id, *offset),
                Some(&payload[..]),
                "pre-checkpoint record survived"
            );
        }
        assert_eq!(reopened.read(7, post), Some(&b"post-checkpoint"[..]));
    }
}
