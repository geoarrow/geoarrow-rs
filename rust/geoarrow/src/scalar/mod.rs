//! GeoArrow scalars, which are references onto a full GeoArrow array at a specific index.

pub use binary::WKB;
pub use coord::{Coord, InterleavedCoord, SeparatedCoord};
pub use geometry::Geometry;
pub(crate) use geometry::OwnedGeometry;
pub use geometrycollection::GeometryCollection;
pub(crate) use geometrycollection::OwnedGeometryCollection;
pub use linestring::LineString;
pub(crate) use linestring::OwnedLineString;
pub use multilinestring::MultiLineString;
pub(crate) use multilinestring::OwnedMultiLineString;
pub use multipoint::MultiPoint;
pub(crate) use multipoint::OwnedMultiPoint;
pub use multipolygon::MultiPolygon;
pub(crate) use multipolygon::OwnedMultiPolygon;
pub(crate) use point::OwnedPoint;
pub use point::Point;
pub(crate) use polygon::OwnedPolygon;
pub use polygon::Polygon;
pub(crate) use rect::OwnedRect;
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
