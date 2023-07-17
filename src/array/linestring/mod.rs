//! Contains the [`LineStringArray`] and [`MutableLineStringArray`] for arrays of LineString
//! geometries.

pub use array::LineStringArray;
pub use iterator::{LineStringArrayValuesIter, LineStringIterator};
pub use mutable::MutableLineStringArray;

mod array;
#[cfg(feature = "geozero")]
mod geozero;
pub mod iterator;
mod mutable;
