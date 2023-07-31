//! Contains the [`PolygonArray`] and [`MutablePolygonArray`] for arrays of Polygon geometries.

#[cfg(feature = "geozero")]
pub use self::geozero::ToGeoArrowPolygonArray;
pub use array::PolygonArray;
pub use iterator::PolygonArrayValuesIter;
pub use mutable::MutablePolygonArray;
pub(crate) use util::parse_polygon;

mod array;
#[cfg(feature = "geozero")]
mod geozero;
pub(crate) mod iterator;
mod mutable;
pub(crate) mod util;
