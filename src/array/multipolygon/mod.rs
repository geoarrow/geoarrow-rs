//! Contains the [`MultiPolygonArray`] and [`MultiPolygonBuilder`] for arrays of MultiPolygon
//! geometries.

pub use array::MultiPolygonArray;
pub use builder::MultiPolygonBuilder;
pub use capacity::MultiPolygonCapacity;

mod array;
mod builder;
mod capacity;
