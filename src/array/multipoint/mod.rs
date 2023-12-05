//! Contains the [`MultiPointArray`] and [`MultiPointBuilder`] for arrays of MultiPoint
//! geometries.

pub use array::MultiPointArray;
pub use builder::MultiPointBuilder;
pub use iterator::MultiPointArrayIter;

mod array;
mod builder;
pub mod iterator;
