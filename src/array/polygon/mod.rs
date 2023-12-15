//! Contains the [`PolygonArray`] and [`PolygonBuilder`] for arrays of Polygon geometries.

pub use array::PolygonArray;
pub use builder::PolygonBuilder;
pub use capacity::PolygonCapacity;
pub use iterator::PolygonArrayIter;
pub(crate) use util::parse_polygon;

mod array;
mod builder;
mod capacity;
pub(crate) mod iterator;
pub(crate) mod util;
