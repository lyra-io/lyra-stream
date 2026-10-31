//! Shared group-committed WAL used by the page-cache stream store.

use super::checkpoint;
use crate::segment::Segment;
use crate::vfs::{Vfs, VfsI};
use bytes::Bytes;
use std::path::{Path, PathBuf};
use std::sync::mpsc::{Receiver, Sender, TryRecvError};

const HEADER_SIZE: usize = 20;
const WAL_SEGMENT_SIZE: u64 = 64 * 1024 * 1024;
const WAL_FILE: &str = "wal.seg";

#[derive(Debug, thiserror::Error)]
pub enum SharedWalError {
    #[error("WAL I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("WAL writer is closed")]
    Closed,
}

/// A record read from the WAL during recovery.
pub struct WalRecord {
    pub stream_id: i64,
    pub offset: u64,
    pub payload: Vec<u8>,
}

enum WalCommand {
    Append {
        stream_id: i64,
        payload: Bytes,
        ack: Sender<Result<u64, SharedWalError>>,
    },
    Checkpoint {
        commit_offset: u64,
        ack: Sender<Result<u64, SharedWalError>>,
    },
}

/// The durability boundary for one stream store.
pub struct SharedWal {
    vfs: VfsI,
    dir: PathBuf,
    tx: Option<Sender<WalCommand>>,
    handle: Option<std::thread::JoinHandle<()>>,
}

impl SharedWal {
    pub fn open(vfs: VfsI, dir: &Path, start_sequence: u64) -> Result<Self, SharedWalError> {
        vfs.create_dir(dir)?;
        let dir = dir.to_path_buf();
        let wal_path = dir.join(WAL_FILE);
        let segment = match Segment::open(&vfs, &wal_path, WAL_SEGMENT_SIZE) {
            Ok(segment) => segment,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                Segment::create(&vfs, &wal_path, WAL_SEGMENT_SIZE)?
            }
            Err(error) => return Err(error.into()),
        };

        let (tx, rx) = std::sync::mpsc::channel();
        let writer_vfs = vfs.clone();
        let writer_dir = dir.clone();
        let handle = std::thread::Builder::new()
            .name("lyra-wal-writer".into())
            .spawn(move || wal_writer_loop(writer_vfs, writer_dir, segment, rx, start_sequence))?;
        Ok(Self {
            vfs,
            dir,
            tx: Some(tx),
            handle: Some(handle),
        })
    }

    pub fn append(&self, stream_id: i64, payload: Bytes) -> Result<u64, SharedWalError> {
        let (ack, rx) = std::sync::mpsc::channel();
        self.tx
            .as_ref()
            .ok_or(SharedWalError::Closed)?
            .send(WalCommand::Append {
                stream_id,
                payload,
                ack,
            })
            .map_err(|_| SharedWalError::Closed)?;
        rx.recv().map_err(|_| SharedWalError::Closed)?
    }

    pub fn checkpoint(&self, commit_offset: impl FnOnce() -> u64) -> Result<u64, SharedWalError> {
        let (ack, rx) = std::sync::mpsc::channel();
        self.tx
            .as_ref()
            .ok_or(SharedWalError::Closed)?
            .send(WalCommand::Checkpoint {
                commit_offset: commit_offset(),
                ack,
            })
            .map_err(|_| SharedWalError::Closed)?;
        rx.recv().map_err(|_| SharedWalError::Closed)?
    }

    pub fn commit_offset(&self) -> Result<u64, SharedWalError> {
        Self::commit_offset_at(&self.vfs, &self.dir)
    }

    pub fn commit_offset_at(vfs: &VfsI, dir: &Path) -> Result<u64, SharedWalError> {
        Ok(checkpoint::read(vfs, dir)?)
    }

    pub fn read_records(&self) -> Result<Vec<WalRecord>, SharedWalError> {
        Self::read_records_at(&self.vfs, &self.dir)
    }

    pub fn read_records_at(vfs: &VfsI, dir: &Path) -> Result<Vec<WalRecord>, SharedWalError> {
        let path = dir.join(WAL_FILE);
        match Segment::open(vfs, &path, WAL_SEGMENT_SIZE) {
            Ok(segment) => read_segment_records(&segment),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(Vec::new()),
            Err(error) => Err(error.into()),
        }
    }
}

impl Drop for SharedWal {
    fn drop(&mut self) {
        self.tx.take();
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

fn wal_writer_loop(
    vfs: VfsI,
    dir: PathBuf,
    mut segment: Segment,
    rx: Receiver<WalCommand>,
    mut sequence: u64,
) {
    let mut batch: Vec<(Sender<Result<u64, SharedWalError>>, u64)> = Vec::new();
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
                let mut frame = Vec::with_capacity(HEADER_SIZE + payload.len());
                encode_frame(&mut frame, stream_id, sequence, &payload);
                if let Err(error) = segment.append(&frame) {
                    let _ = ack.send(Err(SharedWalError::Io(error)));
                    continue;
                }
                batch.push((ack, sequence));
            }
            WalCommand::Checkpoint { commit_offset, ack } => {
                let result = do_checkpoint(&vfs, &dir, &mut segment, commit_offset);
                let _ = ack.send(result);
                continue;
            }
        }

        loop {
            match rx.try_recv() {
                Ok(WalCommand::Append {
                    stream_id,
                    payload,
                    ack,
                }) => {
                    sequence += 1;
                    let mut frame = Vec::with_capacity(HEADER_SIZE + payload.len());
                    encode_frame(&mut frame, stream_id, sequence, &payload);
                    if let Err(error) = segment.append(&frame) {
                        let _ = ack.send(Err(SharedWalError::Io(error)));
                        continue;
                    }
                    batch.push((ack, sequence));
                }
                Ok(command @ WalCommand::Checkpoint { .. }) => {
                    pending = Some(command);
                    break;
                }
                Err(TryRecvError::Empty) | Err(TryRecvError::Disconnected) => break,
            }
        }

        if !batch.is_empty()
            && let Err(error) = segment.sync()
        {
            let kind = error.kind();
            let message = error.to_string();
            for (ack, _) in batch.drain(..) {
                let _ = ack.send(Err(SharedWalError::Io(std::io::Error::new(
                    kind,
                    message.clone(),
                ))));
            }
            continue;
        }
        for (ack, offset) in batch.drain(..) {
            let _ = ack.send(Ok(offset));
        }
    }
}

fn do_checkpoint(
    vfs: &VfsI,
    dir: &Path,
    segment: &mut Segment,
    commit_offset: u64,
) -> Result<u64, SharedWalError> {
    checkpoint::write(vfs, dir, commit_offset)?;

    let records = read_segment_records(segment)?;
    let tmp = dir.join(format!("{WAL_FILE}.tmp"));
    let _ = vfs.remove(&tmp);
    let mut tail = Segment::create(vfs, &tmp, WAL_SEGMENT_SIZE)?;
    for record in records {
        if record.offset >= commit_offset {
            let mut frame = Vec::with_capacity(HEADER_SIZE + record.payload.len());
            encode_frame(&mut frame, record.stream_id, record.offset, &record.payload);
            tail.append(&frame)?;
        }
    }
    tail.sync()?;
    drop(tail);

    let wal_path = dir.join(WAL_FILE);
    let _ = vfs.remove(&wal_path);
    vfs.rename(&tmp, &wal_path)?;
    vfs.sync(dir)?;
    *segment = Segment::open(vfs, &wal_path, WAL_SEGMENT_SIZE)?;
    Ok(commit_offset)
}

fn read_segment_records(segment: &Segment) -> Result<Vec<WalRecord>, SharedWalError> {
    let size = segment.write_position();
    let bytes = segment.read_at(0, size as usize)?;
    Ok(decode_records(&bytes))
}

fn encode_frame(out: &mut Vec<u8>, stream_id: i64, offset: u64, payload: &[u8]) {
    out.extend_from_slice(&stream_id.to_le_bytes());
    out.extend_from_slice(&offset.to_le_bytes());
    out.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    out.extend_from_slice(payload);
}

fn decode_records(mut bytes: &[u8]) -> Vec<WalRecord> {
    let mut records = Vec::new();
    while bytes.len() >= HEADER_SIZE {
        let stream_id = i64::from_le_bytes(bytes[..8].try_into().unwrap());
        let offset = u64::from_le_bytes(bytes[8..16].try_into().unwrap());
        let len = u32::from_le_bytes(bytes[16..20].try_into().unwrap()) as usize;
        if bytes.len() < HEADER_SIZE + len {
            break; // torn tail
        }
        records.push(WalRecord {
            stream_id,
            offset,
            payload: bytes[HEADER_SIZE..HEADER_SIZE + len].to_vec(),
        });
        bytes = &bytes[HEADER_SIZE + len..];
    }
    records
}
