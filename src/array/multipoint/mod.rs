//! Contains the [`MultiPointArray`] and [`MultiPointBuilder`] for arrays of MultiPoint
//! geometries.

pub use array::MultiPointArray;
pub use iterator::MultiPointArrayIter;
pub use mutable::MultiPointBuilder;

mod array;
pub mod iterator;
mod mutable;
