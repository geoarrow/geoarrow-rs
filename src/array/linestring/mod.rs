//! Contains the [`LineStringArray`] and [`LineStringBuilder`] for arrays of LineString
//! geometries.

pub use array::LineStringArray;
pub use iterator::LineStringArrayIter;
pub use mutable::LineStringBuilder;

mod array;
pub mod iterator;
pub(crate) mod mutable;
