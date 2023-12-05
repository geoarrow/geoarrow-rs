//! Contains the [`MultiPolygonArray`] and [`MultiPolygonBuilder`] for arrays of MultiPolygon
//! geometries.

pub use array::MultiPolygonArray;
pub use iterator::MultiPolygonArrayIter;
pub use builder::MultiPolygonBuilder;

mod array;
pub mod iterator;
mod builder;
