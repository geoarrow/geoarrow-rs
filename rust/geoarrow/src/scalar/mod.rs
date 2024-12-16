//! GeoArrow scalars, which are slices of a full GeoArrow array at a specific index.

#![allow(missing_docs)] // FIXME

pub use binary::WKB;
pub use coord::{Coord, InterleavedCoord, SeparatedCoord};
pub use geometry::Geometry;
pub use geometrycollection::GeometryCollection;
pub use linestring::LineString;
pub use multilinestring::MultiLineString;
pub use multipoint::MultiPoint;
pub use multipolygon::MultiPolygon;
pub use point::Point;
pub use polygon::Polygon;
pub use rect::Rect;
pub use scalar::GeometryScalar;

mod binary;
mod coord;
mod geometry;
mod geometrycollection;
mod linestring;
mod multilinestring;
mod multipoint;
mod multipolygon;
mod point;
mod polygon;
mod rect;
#[allow(clippy::module_inception)]
mod scalar;
