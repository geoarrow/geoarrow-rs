//! Contains the [`LineStringArray`] and [`LineStringBuilder`] for arrays of LineString
//! geometries.

pub use array::LineStringArray;
pub use builder::{LineStringBuilder, LineStringCapacity};
pub use iterator::LineStringArrayIter;

mod array;
pub(crate) mod builder;
pub mod iterator;
