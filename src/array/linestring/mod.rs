//! Contains the [`LineStringArray`] and [`LineStringBuilder`] for arrays of LineString
//! geometries.

pub use array::LineStringArray;
pub use builder::LineStringBuilder;
pub use capacity::LineStringCapacity;
pub use iterator::LineStringArrayIter;

mod array;
pub(crate) mod builder;
mod capacity;
pub mod iterator;
