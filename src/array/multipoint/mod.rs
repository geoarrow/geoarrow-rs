//! Contains the [`MultiPointArray`] and [`MultiPointBuilder`] for arrays of MultiPoint
//! geometries.

pub use array::MultiPointArray;
pub use builder::{MultiPointBuilder, MultiPointCapacity};
pub use iterator::MultiPointArrayIter;

mod array;
mod builder;
pub mod iterator;
