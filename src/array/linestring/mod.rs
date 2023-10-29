//! Contains the [`LineStringArray`] and [`MutableLineStringArray`] for arrays of LineString
//! geometries.

pub use array::LineStringArray;
pub use iterator::{LineStringArrayIter, LineStringIterator};
pub use mutable::MutableLineStringArray;

mod array;
pub mod iterator;
pub(crate) mod mutable;
