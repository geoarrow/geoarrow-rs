//! Contains the [`PolygonArray`] and [`PolygonBuilder`] for arrays of Polygon geometries.

pub use array::PolygonArray;
pub use iterator::PolygonArrayIter;
pub use builder::PolygonBuilder;
pub(crate) use util::parse_polygon;

mod array;
pub(crate) mod iterator;
mod builder;
pub(crate) mod util;
