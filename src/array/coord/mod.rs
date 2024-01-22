//! Contains implementations for how to encode arrays of coordinates for all other geometry array
//! types.
//!
//! Coordinates can be either _interleaved_, where they're represented as a `FixedSizeList`, or
//! _separated_, where they're represented with a `StructArray`.

mod combined;
mod interleaved;
mod separated;

pub use combined::{CoordBuffer, CoordBufferBuilder};
pub use interleaved::{InterleavedCoordBuffer, InterleavedCoordBufferBuilder};
pub use separated::{SeparatedCoordBuffer, SeparatedCoordBufferBuilder};

/// The permitted GeoArrow coordinate representations.
///
/// GeoArrow permits coordinate types to either be `Interleaved`, where the X and Y coordinates are
/// in a single buffer as XYXYXY or `Separated`, where the X and Y coordinates are in multiple
/// buffers as XXXX and YYYY.
#[derive(Default, Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CoordType {
    #[default]
    Interleaved,
    Separated,
}
