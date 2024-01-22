//! Contains the [`MultiPointArray`] and [`MultiPointBuilder`] for arrays of MultiPoint
//! geometries.

pub use array::MultiPointArray;
pub use builder::MultiPointBuilder;
pub use capacity::MultiPointCapacity;

mod array;
mod builder;
mod capacity;
