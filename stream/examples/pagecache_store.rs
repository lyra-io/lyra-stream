//! Demonstrates the page-cache stream storage:
//!
//! - appends are acknowledged only after the shared WAL is durable
//! - reads are served from the raw segment files through the VFS
//! - checkpoint asks the WAL to record and trim at the stream commit offset
//! - reopening recovers pre-checkpoint records from the index snapshot and the
//!   post-checkpoint tail from the WAL

use bytes::Bytes;
use lyra_stream::streams::{PageCacheStream, StreamOptions, StreamStorage};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = std::env::temp_dir().join("lyra-pagecache-example");
    let _ = std::fs::remove_dir_all(&dir);

    let known = {
        let storage = PageCacheStream::open_standard(&dir, StreamOptions::default())?;
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

    let storage = PageCacheStream::open_standard(&dir, StreamOptions::default())?;
    for (stream_id, offset, expected) in &known {
        let payload = storage.read(*stream_id, *offset).expect("record survived");
        assert_eq!(payload.as_ref(), &expected[..]);
        println!("recovered stream={stream_id} offset={offset}: {expected:?}");
    }

    Ok(())
}
