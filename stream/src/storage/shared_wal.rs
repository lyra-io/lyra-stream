//! The shared, group-committed write-ahead log.

use crate::storage::error::StorageError;
use bytes::Bytes;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::mpsc::{Receiver, Sender, TryRecvError};

const HEADER_SIZE: usize = 20;

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
        ack: Sender<Result<u64, StorageError>>,
    },
    Trim {
        commit_offset: u64,
        ack: Sender<Result<(), StorageError>>,
    },
}

/// A background writer batches appends and resolves them after one
/// `fdatasync`. Trims are executed by the same thread so the append handle
/// stays valid across file rotation.
pub struct SharedWal {
    tx: Option<Sender<WalCommand>>,
    handle: Option<std::thread::JoinHandle<()>>,
}

impl SharedWal {
    pub fn open(path: &Path, start_sequence: u64) -> Result<Self, StorageError> {
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

    pub fn append(&self, stream_id: i64, payload: Bytes) -> Result<u64, StorageError> {
        let (ack, rx) = std::sync::mpsc::channel();
        self.tx
            .as_ref()
            .ok_or(StorageError::WalClosed)?
            .send(WalCommand::Append {
                stream_id,
                payload,
                ack,
            })
            .map_err(|_| StorageError::WalClosed)?;
        rx.recv().map_err(|_| StorageError::WalClosed)?
    }

    pub fn trim(&self, commit_offset: u64) -> Result<(), StorageError> {
        let (ack, rx) = std::sync::mpsc::channel();
        self.tx
            .as_ref()
            .ok_or(StorageError::WalClosed)?
            .send(WalCommand::Trim { commit_offset, ack })
            .map_err(|_| StorageError::WalClosed)?;
        rx.recv().map_err(|_| StorageError::WalClosed)?
    }

    pub fn read_records(path: &Path) -> Result<Vec<WalRecord>, StorageError> {
        match File::open(path) {
            Ok(mut file) => read_wal_records(&mut file),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(Vec::new()),
            Err(error) => Err(StorageError::Io(error)),
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

fn wal_writer_loop(path: PathBuf, mut file: File, rx: Receiver<WalCommand>, mut sequence: u64) {
    let mut batch: Vec<(Sender<Result<u64, StorageError>>, u64)> = Vec::new();
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
                let mut record = Vec::with_capacity(HEADER_SIZE + payload.len());
                append_wal_record(&mut record, stream_id, sequence, &payload);
                if let Err(error) = file.write_all(&record) {
                    let _ = ack.send(Err(StorageError::Io(error)));
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

        // Group commit: drain the queued appends, but leave trims in order.
        loop {
            match rx.try_recv() {
                Ok(WalCommand::Append {
                    stream_id,
                    payload,
                    ack,
                }) => {
                    sequence += 1;
                    let mut record = Vec::with_capacity(HEADER_SIZE + payload.len());
                    append_wal_record(&mut record, stream_id, sequence, &payload);
                    if let Err(error) = file.write_all(&record) {
                        let _ = ack.send(Err(StorageError::Io(error)));
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

        if !batch.is_empty()
            && let Err(error) = file.sync_data()
        {
            let kind = error.kind();
            let message = error.to_string();
            for (ack, _) in batch.drain(..) {
                let _ = ack.send(Err(StorageError::Io(std::io::Error::new(
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

fn trim_wal_file(path: &Path, file: &mut File, commit_offset: u64) -> Result<(), StorageError> {
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

fn read_wal_records(file: &mut File) -> Result<Vec<WalRecord>, StorageError> {
    file.seek(SeekFrom::Start(0))?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)?;
    let mut records = Vec::new();
    let mut position = 0usize;
    while position + HEADER_SIZE <= bytes.len() {
        let stream_id = i64::from_le_bytes(bytes[position..position + 8].try_into().unwrap());
        let offset = u64::from_le_bytes(bytes[position + 8..position + 16].try_into().unwrap());
        let len =
            u32::from_le_bytes(bytes[position + 16..position + 20].try_into().unwrap()) as usize;
        if position + HEADER_SIZE + len > bytes.len() {
            break; // torn tail from an unclean shutdown
        }
        let payload = bytes[position + HEADER_SIZE..position + HEADER_SIZE + len].to_vec();
        records.push(WalRecord {
            stream_id,
            offset,
            payload,
        });
        position += HEADER_SIZE + len;
    }
    Ok(records)
}
