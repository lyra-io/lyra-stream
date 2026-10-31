//! Demonstrates the real storage engine:
//!
//! - appends are acknowledged only after the shared WAL is durable
//! - reads are served from the mmap'd page-cache segments
//! - a checkpoint snapshots the index and trims the WAL
//! - reopening recovers pre-checkpoint records from the data store and the
//!   post-checkpoint tail from the WAL

use bytes::Bytes;
use lyra_stream::storage::{Storage, StorageOptions};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = std::env::temp_dir().join("lyra-pagecache-example");
    let _ = std::fs::remove_dir_all(&dir);

    let known = {
        let storage = Storage::open(&dir, StorageOptions::default())?;
        let a = storage.append(1, Bytes::from_static(b"hello"))?;
        let b = storage.append(2, Bytes::from_static(b"world"))?;
        let c = storage.append(1, Bytes::from_static(b"!"))?;

        assert_eq!(storage.read(1, a), Some(Bytes::from_static(b"hello")));
        assert_eq!(storage.read(2, b), Some(Bytes::from_static(b"world")));

        storage.checkpoint()?;
        vec![
            (1, a, b"hello".to_vec()),
            (2, b, b"world".to_vec()),
            (1, c, b"!".to_vec()),
        ]
    };

    let storage = Storage::open(&dir, StorageOptions::default())?;
    for (stream_id, offset, expected) in &known {
        let payload = storage.read(*stream_id, *offset).expect("record survived");
        assert_eq!(payload.as_ref(), &expected[..]);
        println!("recovered stream={stream_id} offset={offset}: {expected:?}");
    }

    Ok(())
}
