//! Contains the [`MultiPolygonArray`] and [`MultiPolygonBuilder`] for arrays of MultiPolygon
//! geometries.

pub use array::MultiPolygonArray;
pub use iterator::MultiPolygonArrayIter;
pub use mutable::MultiPolygonBuilder;

mod array;
pub mod iterator;
mod mutable;
