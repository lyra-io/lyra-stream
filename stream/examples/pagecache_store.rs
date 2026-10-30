//! Standalone demonstration of the storage model:
//!
//! - a shared append-only WAL (standard buffered I/O + `fdatasync`; swap in the
//!   `DirectVfs` for O_DIRECT where supported)
//! - an mmap'd data file served by the page cache
//! - an in-memory `(stream_id, offset) -> (position, length)` index
//!
//! The WAL is the durability boundary and the data file is rebuilt from it on
//! startup, so the data file is a pure page-cache materialization.

use memmap2::MmapMut;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

const SEGMENT_SIZE: usize = 64 * 1024 * 1024;
const WAL_HEADER_SIZE: usize = 20;

/// Append-only, shared across every stream. Records are `stream_id | offset |
/// len | payload` so the data file and index can be rebuilt after a crash.
struct Wal {
    file: File,
    sequence: u64,
}

impl Wal {
    fn open(dir: &Path) -> std::io::Result<Self> {
        std::fs::create_dir_all(dir)?;
        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(dir.join("wal.log"))?;

        // Recover the high-water mark from the existing records.
        let mut sequence = 0u64;
        for record in read_records(&mut file)? {
            sequence = record.offset;
        }
        Ok(Self { file, sequence })
    }

    fn append(&mut self, stream_id: i64, payload: &[u8]) -> std::io::Result<u64> {
        let offset = self.sequence + 1;
        let mut record = Vec::with_capacity(WAL_HEADER_SIZE + payload.len());
        record.extend_from_slice(&stream_id.to_le_bytes());
        record.extend_from_slice(&offset.to_le_bytes());
        record.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        record.extend_from_slice(payload);
        self.file.write_all(&record)?;
        // fdatasync makes the ack durable. Group commit would batch this.
        self.file.sync_data()?;
        self.sequence = offset;
        Ok(offset)
    }
}

struct Record {
    stream_id: i64,
    offset: u64,
    payload: Vec<u8>,
}

fn read_records(file: &mut File) -> std::io::Result<Vec<Record>> {
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
            break; // torn tail; the next fsync/append supersedes it
        }
        let payload = bytes[position + WAL_HEADER_SIZE..position + WAL_HEADER_SIZE + len].to_vec();
        records.push(Record {
            stream_id,
            offset,
            payload,
        });
        position += WAL_HEADER_SIZE + len;
    }
    Ok(records)
}

/// mmap'd data file. Writes are `memcpy` into the mapping; reads are zero-copy
/// slices into the same mapping, all served by the OS page cache.
struct DataStore {
    map: MmapMut,
    position: usize,
}

impl DataStore {
    fn new(path: &Path) -> std::io::Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(true)
            .open(path)?;
        file.set_len(SEGMENT_SIZE as u64)?;
        // SAFETY: the file is owned by this process and only this struct writes.
        let map = unsafe { MmapMut::map_mut(&file)? };
        Ok(Self { map, position: 0 })
    }

    fn append(&mut self, payload: &[u8]) -> (u64, u32) {
        let position = self.position as u64;
        let len = payload.len() as u32;
        self.map[self.position..self.position + payload.len()].copy_from_slice(payload);
        self.position += payload.len();
        (position, len)
    }

    fn read(&self, position: u64, len: u32) -> &[u8] {
        let start = position as usize;
        &self.map[start..start + len as usize]
    }
}

struct Store {
    wal: Wal,
    data: DataStore,
    index: HashMap<(i64, u64), (u64, u32)>,
}

impl Store {
    fn open(dir: &Path) -> std::io::Result<Self> {
        let wal = Wal::open(dir)?;
        let mut data = DataStore::new(&dir.join("data.mmap"))?;
        let mut index = HashMap::new();

        // Rebuild the data file and index from the durable WAL.
        let mut replay = wal.file.try_clone()?;
        for record in read_records(&mut replay)? {
            let (position, len) = data.append(&record.payload);
            index.insert((record.stream_id, record.offset), (position, len));
        }

        Ok(Store { wal, data, index })
    }

    fn append(&mut self, stream_id: i64, payload: &[u8]) -> std::io::Result<u64> {
        let offset = self.wal.append(stream_id, payload)?;
        let (position, len) = self.data.append(payload);
        self.index.insert((stream_id, offset), (position, len));
        Ok(offset)
    }

    fn read(&self, stream_id: i64, offset: u64) -> Option<&[u8]> {
        let (position, len) = self.index.get(&(stream_id, offset))?;
        Some(self.data.read(*position, *len))
    }
}

fn main() -> std::io::Result<()> {
    let dir = std::env::temp_dir().join("lyra-pagecache-example");
    let _ = std::fs::remove_dir_all(&dir); // start clean across runs
    let mut known = Vec::new();

    // Write phase.
    {
        let mut store = Store::open(&dir)?;
        let a = store.append(1, b"hello")?;
        let b = store.append(2, b"world")?;
        let c = store.append(1, b"!")?;
        assert_eq!(store.read(1, a), Some(&b"hello"[..]));
        assert_eq!(store.read(2, b), Some(&b"world"[..]));
        known.push((1, a, b"hello".to_vec()));
        known.push((2, b, b"world".to_vec()));
        known.push((1, c, b"!".to_vec()));
        println!("wrote 3 records across 2 streams");
    }

    // Recovery phase: reopen and read the same records back from the WAL replay.
    {
        let store = Store::open(&dir)?;
        println!(
            "reopened: {} records rebuilt from the WAL",
            store.index.len()
        );
        for (stream_id, offset, expected) in &known {
            assert_eq!(store.read(*stream_id, *offset), Some(&expected[..]));
            println!(
                "recovered stream={stream_id} offset={offset}: {:?}",
                expected
            );
        }
    }

    Ok(())
}
