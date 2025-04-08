//! The concrete array definitions.
//!
//! All arrays implement the core [GeoArrowArray][crate::GeoArrowArray] trait.

mod coord;
mod geometry;
mod geometrycollection;
mod linestring;
mod mixed;
mod multilinestring;
mod multipoint;
mod multipolygon;
mod point;
mod polygon;
mod rect;
mod wkb;
mod wkt;

pub use coord::{CoordBuffer, InterleavedCoordBuffer, SeparatedCoordBuffer};
pub use geometry::GeometryArray;
pub use geometrycollection::GeometryCollectionArray;
pub use linestring::LineStringArray;
pub(crate) use mixed::MixedGeometryArray;
pub use multilinestring::MultiLineStringArray;
pub use multipoint::MultiPointArray;
pub use multipolygon::MultiPolygonArray;
pub use point::PointArray;
pub use polygon::PolygonArray;
pub use rect::RectArray;
pub use wkb::WKBArray;
pub use wkt::WKTArray;
