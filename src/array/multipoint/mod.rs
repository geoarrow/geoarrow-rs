//! Contains the [`MultiPointArray`] and [`MutableMultiPointArray`] for arrays of MultiPoint
//! geometries.

pub use array::MultiPointArray;
pub use iterator::MultiPointArrayValuesIter;
pub use mutable::MutableMultiPointArray;

mod array;
pub mod iterator;
mod mutable;
