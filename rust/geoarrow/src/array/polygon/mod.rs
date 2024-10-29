//! Contains the [`PolygonArray`] and [`PolygonBuilder`] for arrays of Polygon geometries.

pub use array::PolygonArray;
pub use builder::PolygonBuilder;
pub use capacity::PolygonCapacity;

mod array;
mod builder;
mod capacity;
