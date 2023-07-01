//! Layout for arrays of coordinates
//!
//! Coordinates can be either _interleaved_, where they're represented as a `FixedSizeList`, or
//! _separated_, where they're represented with a `StructArray`.

pub mod combined;
pub mod interleaved;
pub mod separated;

pub use combined::{Coord, CoordArray};
pub use interleaved::{InterleavedCoord, InterleavedCoordArray};
pub use separated::{SeparatedCoord, SeparatedCoordArray};
