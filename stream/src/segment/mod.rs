//! Shared segment files used by both the WAL and stream storage.
//!
//! The on-disk segment format lives in [`crate::format::segment`].

mod file;
mod set;

pub(crate) use crate::format::segment::{list_segments, make_segment_path};
pub use file::Segment;
pub use set::SegmentSet;

/// Physical position of a buffer inside the segment set.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Position {
    pub segment: u32,
    pub offset: u32,
    pub len: u32,
}
