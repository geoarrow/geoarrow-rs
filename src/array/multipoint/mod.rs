//! Contains the [`MultiPointArray`] and [`MutableMultiPointArray`] for arrays of MultiPoint
//! geometries.

pub use array::MultiPointArray;
pub use iterator::{MultiPointArrayValuesIter, MultiPointIterator};
pub use mutable::MutableMultiPointArray;

mod array;
#[cfg(feature = "geozero")]
mod geozero;
pub mod iterator;
mod mutable;
