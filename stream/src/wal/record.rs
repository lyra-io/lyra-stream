//! WAL record framing.
//!
//! A WAL segment contains zero or more records laid out back to back:
//!
//! ```text
//! ┌──────────────┬──────────────┬───────────────┬──────────────────────────┐
//! │ stream_id    │ offset       │ payload_len   │ payload                  │
//! │ i64 LE  (8B) │ u64 LE (8B)  │ u32 LE  (4B)  │ payload_len bytes        │
//! └──────────────┴──────────────┴───────────────┴──────────────────────────┘
//! ```
//!
//! A torn final record is discarded during recovery. There is deliberately no
//! CRC or block framing here; the segment itself only stores raw buffers and
//! durability is provided by the VFS sync path.

pub(crate) const HEADER_SIZE: usize = 20;

/// A logical WAL record read during recovery.
pub struct WalRecord {
    pub stream_id: i64,
    pub offset: u64,
    pub payload: Vec<u8>,
}

pub(crate) fn encode_record(out: &mut Vec<u8>, stream_id: i64, offset: u64, payload: &[u8]) {
    out.extend_from_slice(&stream_id.to_le_bytes());
    out.extend_from_slice(&offset.to_le_bytes());
    out.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    out.extend_from_slice(payload);
}

pub(crate) fn decode_records(mut bytes: &[u8]) -> Vec<WalRecord> {
    let mut records = Vec::new();
    while bytes.len() >= HEADER_SIZE {
        let stream_id = i64::from_le_bytes(bytes[..8].try_into().unwrap());
        let offset = u64::from_le_bytes(bytes[8..16].try_into().unwrap());
        let len = u32::from_le_bytes(bytes[16..20].try_into().unwrap()) as usize;
        if bytes.len() < HEADER_SIZE + len {
            break; // torn tail from an unclean shutdown
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
