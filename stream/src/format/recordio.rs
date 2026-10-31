//! RocksDB-style WAL record framing.
//!
//! Records are split into physical fragments and laid out in 32 KiB blocks:
//!
//! ```text
//! Segment file
//! ┌─────────────────────────────────────────────────────────────┐
//! │ 32 KiB record block                                        │
//! │ ┌──────────┬───────┬──────┬───────────┬──────────────────┐ │
//! │ │ CRC32C:4 │ len:2 │ type │ segment:4 │ fragment payload │ │
//! │ └──────────┴───────┴──────┴───────────┴──────────────────┘ │
//! │ ... FULL or FIRST / MIDDLE / LAST fragments ...             │
//! │ zero trailer when fewer than 11 bytes remain in the block   │
//! ├─────────────────────────────────────────────────────────────┤
//! │ next 32 KiB record block                                    │
//! └─────────────────────────────────────────────────────────────┘
//! ```

use bytes::Bytes;
use std::io::Read;
use std::path::Path;

pub const BLOCK_SIZE: usize = 32 * 1024;
pub const HEADER_SIZE: usize = 11;

const MASK_DELTA: u32 = 0xa282_ead8;

#[derive(Debug, thiserror::Error)]
pub enum RecordIoError {
    #[error("recordio I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("recordio format error: {0}")]
    Format(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
enum RecordType {
    Full = 0,
    First = 1,
    Middle = 2,
    Last = 3,
}

impl RecordType {
    fn decode(value: u8) -> Result<Self, RecordIoError> {
        match value {
            0 => Ok(Self::Full),
            1 => Ok(Self::First),
            2 => Ok(Self::Middle),
            3 => Ok(Self::Last),
            _ => Err(RecordIoError::Format(format!(
                "invalid physical record type {value}"
            ))),
        }
    }
}

pub fn encode_record(
    segment_number: u64,
    start_position: u64,
    payload: &[u8],
) -> Result<Vec<u8>, RecordIoError> {
    let mut output = Vec::with_capacity(payload.len().saturating_add(HEADER_SIZE));
    encode_record_parts(&mut output, segment_number, start_position, &[], payload)?;
    Ok(output)
}

pub fn encode_record_parts(
    output: &mut Vec<u8>,
    segment_number: u64,
    start_position: u64,
    prefix: &[u8],
    payload: &[u8],
) -> Result<(), RecordIoError> {
    let segment_number = u32::try_from(segment_number).map_err(|_| {
        RecordIoError::Format(format!("segment number {segment_number} exceeds u32"))
    })?;
    let output_start = output.len();
    let payload_size = prefix
        .len()
        .checked_add(payload.len())
        .ok_or_else(|| RecordIoError::Format("record size overflows usize".to_string()))?;
    let mut consumed = 0usize;
    let mut first = true;

    loop {
        let absolute_position = start_position
            .checked_add(
                u64::try_from(output.len() - output_start)
                    .map_err(|_| RecordIoError::Format("position does not fit u64".to_string()))?,
            )
            .ok_or_else(|| RecordIoError::Format("position overflow".to_string()))?;
        let position_in_block = usize::try_from(absolute_position % BLOCK_SIZE as u64)
            .map_err(|_| RecordIoError::Format("position does not fit usize".to_string()))?;
        let block_remaining = BLOCK_SIZE - position_in_block;
        if block_remaining < HEADER_SIZE {
            output.resize(output.len() + block_remaining, 0);
            continue;
        }

        let available = block_remaining - HEADER_SIZE;
        let fragment_size = (payload_size - consumed).min(available);
        let last = consumed + fragment_size == payload_size;
        let record_type = match (first, last) {
            (true, true) => RecordType::Full,
            (true, false) => RecordType::First,
            (false, true) => RecordType::Last,
            (false, false) => RecordType::Middle,
        };
        encode_fragment(
            output,
            record_type,
            segment_number,
            prefix,
            payload,
            consumed,
            fragment_size,
        );
        consumed += fragment_size;
        if last {
            return Ok(());
        }
        first = false;
    }
}

fn encode_fragment(
    output: &mut Vec<u8>,
    record_type: RecordType,
    segment_number: u32,
    prefix: &[u8],
    payload: &[u8],
    consumed: usize,
    fragment_size: usize,
) {
    let header_start = output.len();
    output.resize(header_start + HEADER_SIZE, 0);
    let fragment_end = consumed + fragment_size;
    if consumed < prefix.len() {
        output.extend_from_slice(&prefix[consumed..fragment_end.min(prefix.len())]);
    }
    if fragment_end > prefix.len() {
        let payload_start = consumed.saturating_sub(prefix.len());
        let payload_end = fragment_end - prefix.len();
        output.extend_from_slice(&payload[payload_start..payload_end]);
    }

    let fragment_start = header_start + HEADER_SIZE;
    let checksum = calculate_checksum(record_type as u8, segment_number, &output[fragment_start..]);
    output[header_start..header_start + 4].copy_from_slice(&checksum.to_le_bytes());
    output[header_start + 4..header_start + 6]
        .copy_from_slice(&(fragment_size as u16).to_le_bytes());
    output[header_start + 6] = record_type as u8;
    output[header_start + 7..header_start + 11].copy_from_slice(&segment_number.to_le_bytes());
}

pub fn decode_record(
    reader: &mut impl Read,
    _path: &Path,
    segment_number: u64,
    start_position: u64,
    file_end: u64,
) -> Result<(u64, Bytes), RecordIoError> {
    let expected_segment_number = u32::try_from(segment_number)
        .map_err(|_| RecordIoError::Format("segment number exceeds u32".to_string()))?;
    let mut position = start_position;
    let mut fragments = Vec::new();
    let mut fragmented = false;

    loop {
        if position >= file_end {
            return Err(RecordIoError::Format(
                "incomplete record at end of file".to_string(),
            ));
        }

        let position_in_block = usize::try_from(position % BLOCK_SIZE as u64)
            .map_err(|_| RecordIoError::Format("position does not fit usize".to_string()))?;
        let block_remaining = BLOCK_SIZE - position_in_block;
        let file_remaining = file_end - position;
        if block_remaining < HEADER_SIZE {
            let trailer_size = usize::try_from(file_remaining.min(block_remaining as u64))
                .map_err(|_| RecordIoError::Format("trailer does not fit usize".to_string()))?;
            let mut trailer = vec![0; trailer_size];
            reader.read_exact(&mut trailer)?;
            if !trailer.iter().all(|byte| *byte == 0) {
                return Err(RecordIoError::Format(
                    "non-zero bytes in a block trailer".to_string(),
                ));
            }
            position += trailer_size as u64;
            continue;
        }

        let (record_type, fragment, consumed) = decode_fragment(
            reader,
            expected_segment_number,
            block_remaining,
            file_remaining,
        )?;
        position += consumed;
        match record_type {
            RecordType::Full => {
                if fragmented {
                    return Err(RecordIoError::Format(
                        "full record found inside fragmented record".to_string(),
                    ));
                }
                return Ok((position, fragment));
            }
            RecordType::First => {
                if fragmented {
                    return Err(RecordIoError::Format(
                        "first fragment found inside fragmented record".to_string(),
                    ));
                }
                fragmented = true;
                fragments.extend_from_slice(&fragment);
            }
            RecordType::Middle => {
                if !fragmented {
                    return Err(RecordIoError::Format(
                        "middle fragment without first fragment".to_string(),
                    ));
                }
                fragments.extend_from_slice(&fragment);
            }
            RecordType::Last => {
                if !fragmented {
                    return Err(RecordIoError::Format(
                        "last fragment without first fragment".to_string(),
                    ));
                }
                fragments.extend_from_slice(&fragment);
                return Ok((position, Bytes::from(fragments)));
            }
        }
    }
}

fn decode_fragment(
    reader: &mut impl Read,
    expected_segment_number: u32,
    block_remaining: usize,
    file_remaining: u64,
) -> Result<(RecordType, Bytes, u64), RecordIoError> {
    if file_remaining < HEADER_SIZE as u64 {
        return Err(RecordIoError::Format(
            "truncated physical record header".to_string(),
        ));
    }
    let mut header = [0; HEADER_SIZE];
    reader.read_exact(&mut header)?;
    if header.iter().all(|byte| *byte == 0) {
        return Err(RecordIoError::Format(
            "zeroed physical record header".to_string(),
        ));
    }

    let expected_checksum = u32::from_le_bytes(header[..4].try_into().unwrap());
    let payload_size = u16::from_le_bytes(header[4..6].try_into().unwrap()) as usize;
    let record_type_byte = header[6];
    let segment_number = u32::from_le_bytes(header[7..11].try_into().unwrap());
    let record_size = HEADER_SIZE + payload_size;
    if record_size > block_remaining {
        return Err(RecordIoError::Format(
            "physical record crosses a block boundary".to_string(),
        ));
    }
    if record_size as u64 > file_remaining {
        return Err(RecordIoError::Format(
            "truncated physical record body".to_string(),
        ));
    }
    if segment_number != expected_segment_number {
        return Err(RecordIoError::Format(
            "physical record segment number mismatch".to_string(),
        ));
    }

    let record_type = RecordType::decode(record_type_byte)?;
    let mut payload = vec![0; payload_size];
    reader.read_exact(&mut payload)?;
    if calculate_checksum(record_type_byte, segment_number, &payload) != expected_checksum {
        return Err(RecordIoError::Format(
            "physical record checksum mismatch".to_string(),
        ));
    }

    Ok((record_type, Bytes::from(payload), record_size as u64))
}

fn calculate_checksum(record_type: u8, segment_number: u32, payload: &[u8]) -> u32 {
    let checksum = crc32c::crc32c(&[record_type]);
    let checksum = crc32c::crc32c_append(checksum, &segment_number.to_le_bytes());
    let checksum = crc32c::crc32c_append(checksum, payload);
    checksum.rotate_right(15).wrapping_add(MASK_DELTA)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn record_round_trips_across_blocks() {
        let payload = vec![0xAB; BLOCK_SIZE * 2 + 113];
        let encoded = encode_record(1, 0, &payload).unwrap();
        let file_end = encoded.len() as u64;
        let (next, decoded) = decode_record(
            &mut Cursor::new(encoded),
            Path::new("segment"),
            1,
            0,
            file_end,
        )
        .unwrap();
        assert_eq!(next, file_end);
        assert_eq!(decoded, payload);
    }
}
