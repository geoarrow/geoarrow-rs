//! Contains the [`MultiPointArray`] and [`MutableMultiPointArray`] for arrays of MultiPoint
//! geometries.

pub use array::MultiPointArray;
pub use iterator::MultiPointArrayIter;
pub use mutable::MutableMultiPointArray;

mod array;
pub mod iterator;
mod mutable;
