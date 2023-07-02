//! Layout for arrays of coordinates
//!
//! Coordinates can be either _interleaved_, where they're represented as a `FixedSizeList`, or
//! _separated_, where they're represented with a `StructArray`.

pub mod combined;
pub mod interleaved;
pub mod separated;

pub use combined::{CoordBuffer, MutableCoordBuffer};
pub use interleaved::{InterleavedCoordBuffer, MutableInterleavedCoordBuffer};
pub use separated::{MutableSeparatedCoordBuffer, SeparatedCoordBuffer};
