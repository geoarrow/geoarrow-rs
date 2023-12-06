//! Contains the [`PolygonArray`] and [`PolygonBuilder`] for arrays of Polygon geometries.

pub use array::PolygonArray;
pub use builder::{PolygonBuilder, PolygonCapacity};
pub use iterator::PolygonArrayIter;
pub(crate) use util::parse_polygon;

mod array;
mod builder;
pub(crate) mod iterator;
pub(crate) mod util;
