//! Contains the [`PolygonArray`] and [`MutablePolygonArray`] for arrays of Polygon geometries.

pub use array::PolygonArray;
pub use iterator::PolygonArrayValuesIter;
pub use mutable::MutablePolygonArray;
pub(crate) use util::parse_polygon;

mod array;
pub(crate) mod iterator;
mod mutable;
pub(crate) mod util;
