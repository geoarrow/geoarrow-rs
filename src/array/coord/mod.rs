//! Contains implementations for how to encode arrays of coordinates for all other geometry array
//! types.
//!
//! Coordinates can be either _interleaved_, where they're represented as a `FixedSizeList`, or
//! _separated_, where they're represented with a `StructArray`.

pub mod combined;
pub mod interleaved;
pub mod separated;

pub use combined::{CoordBuffer, CoordBufferBuilder};
pub use interleaved::{InterleavedCoordBuffer, InterleavedCoordBufferBuilder};
pub use separated::{SeparatedCoordBuffer, SeparatedCoordBufferBuilder};

#[derive(Default, Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CoordType {
    #[default]
    Interleaved,
    Separated,
}
