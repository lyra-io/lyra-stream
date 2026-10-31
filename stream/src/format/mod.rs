//! On-disk format definitions shared by the WAL and stream storage.
//!
//! The physical layout is split into three pieces:
//!
//! - [`segment`]: fixed-size raw-buffer segment files and their filename rules.
//! - [`record`]: the simple length-prefixed WAL record framing.
//! - [`recordio`]: optional RocksDB-style block/CRC record framing.

pub mod record;
pub mod recordio;
pub mod segment;

pub use record::{HEADER_SIZE, WalRecord, decode_records, encode_record};
pub use segment::{SEGMENT_EXTENSION, SEGMENT_NAME_LEN};
