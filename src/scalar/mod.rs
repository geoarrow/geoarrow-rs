//! GeoArrow scalars, which are references onto a full GeoArrow array at a specific index.

pub use binary::{OwnedWKB, WKB};
pub use coord::{Coord, InterleavedCoord, SeparatedCoord};
pub use geometry::{Geometry, OwnedGeometry};
pub use geometrycollection::{GeometryCollection, OwnedGeometryCollection};
pub use linestring::{LineString, OwnedLineString};
pub use multilinestring::{MultiLineString, OwnedMultiLineString};
pub use multipoint::{MultiPoint, OwnedMultiPoint};
pub use multipolygon::{MultiPolygon, OwnedMultiPolygon};
pub use point::{OwnedPoint, Point};
pub use polygon::{OwnedPolygon, Polygon};
pub use rect::{OwnedRect, Rect};

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
