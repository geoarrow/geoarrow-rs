//! Contains the [`LineStringArray`] and [`MutableLineStringArray`] for arrays of LineString
//! geometries.

pub use array::LineStringArray;
pub use iterator::{LineStringArrayValuesIter, LineStringIterator};
pub use mutable::MutableLineStringArray;

mod array;
pub mod iterator;
mod mutable;
