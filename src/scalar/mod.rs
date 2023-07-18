//! Contains implementations of GeoArrow scalars, which are references onto a full GeoArrow array
//! at a specific index.

pub use binary::WKB;
pub use coord::{Coord, InterleavedCoord, SeparatedCoord};
pub use geometry::Geometry;
pub use linestring::LineString;
pub use multilinestring::MultiLineString;
pub use multipoint::MultiPoint;
pub use multipolygon::MultiPolygon;
pub use point::Point;
pub use polygon::Polygon;

pub mod binary;
pub mod coord;
pub mod geometry;
pub mod linestring;
pub mod multilinestring;
pub mod multipoint;
pub mod multipolygon;
pub mod point;
pub mod polygon;
