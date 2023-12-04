//! Contains implementations of GeoArrow scalars, which are references onto a full GeoArrow array
//! at a specific index.

pub use binary::WKB;
pub use coord::{Coord, InterleavedCoord, SeparatedCoord};
pub use geometry::Geometry;
pub use geometrycollection::GeometryCollection;
pub use linestring::{LineString, OwnedLineString};
pub use multilinestring::{MultiLineString, OwnedMultiLineString};
pub use multipoint::{MultiPoint, OwnedMultiPoint};
pub use multipolygon::{MultiPolygon, OwnedMultiPolygon};
pub use point::{OwnedPoint, Point};
pub use polygon::{OwnedPolygon, Polygon};
pub use rect::Rect;

pub mod binary;
pub mod coord;
pub mod geometry;
pub mod geometrycollection;
pub mod linestring;
pub mod multilinestring;
pub mod multipoint;
pub mod multipolygon;
pub mod point;
pub mod polygon;
pub mod rect;
pub mod geometry_scalar_ref;