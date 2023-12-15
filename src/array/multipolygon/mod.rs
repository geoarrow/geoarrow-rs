//! Contains the [`MultiPolygonArray`] and [`MultiPolygonBuilder`] for arrays of MultiPolygon
//! geometries.

pub use array::MultiPolygonArray;
pub use builder::MultiPolygonBuilder;
pub use capacity::MultiPolygonCapacity;
pub use iterator::MultiPolygonArrayIter;

mod array;
mod builder;
mod capacity;
pub mod iterator;
