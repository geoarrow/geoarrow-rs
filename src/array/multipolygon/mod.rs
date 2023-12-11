//! Contains the [`MultiPolygonArray`] and [`MultiPolygonBuilder`] for arrays of MultiPolygon
//! geometries.

pub use array::MultiPolygonArray;
pub use builder::{MultiPolygonBuilder, MultiPolygonCapacity};
pub use iterator::MultiPolygonArrayIter;

mod array;
mod builder;
pub mod iterator;
