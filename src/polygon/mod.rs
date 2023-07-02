//! Helpers for using Polygon GeoArrow data

pub use array::PolygonArray;
pub use mutable::MutablePolygonArray;
pub use scalar::Polygon;
pub(crate) use util::parse_polygon;

mod array;
#[cfg(feature = "geozero")]
mod geozero;
mod iterator;
mod mutable;
mod scalar;
#[cfg(test)]
pub(crate) mod test;
pub(crate) mod util;
