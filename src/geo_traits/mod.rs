//! An initial, in-progress implementation of [geometry access
//! traits](https://github.com/georust/geo/discussions/838).
//!
//! The idea is that functions should be able to operate on and consume geospatial vector data from
//! _any_ source without overhead, not limited to just the layout defined in the [`geo`] crate.
//!
//! The main work for this is happening in the [`geo`] repository (see
//! [here](https://github.com/georust/geo/pull/1019)) but that is vendored into this repository for
//! use internally, such as in the WKB parser.

pub use coord::{CoordTrait, UnimplementedCoord};
pub use dimension::Dimensions;
pub use geometry::{GeometryTrait, GeometryType};
pub use geometry_collection::GeometryCollectionTrait;
pub use line::{LineTrait, UnimplementedLine};
pub use line_string::{LineStringTrait, UnimplementedLineString};
pub use multi_line_string::{MultiLineStringTrait, UnimplementedMultiLineString};
pub use multi_point::{MultiPointTrait, UnimplementedMultiPoint};
pub use multi_polygon::{MultiPolygonTrait, UnimplementedMultiPolygon};
pub use point::{PointTrait, UnimplementedPoint};
pub use polygon::{PolygonTrait, UnimplementedPolygon};
pub use rect::{RectTrait, UnimplementedRect};
pub use triangle::{TriangleTrait, UnimplementedTriangle};

mod coord;
mod dimension;
mod geometry;
mod geometry_collection;
mod iterator;
mod line;
mod line_string;
mod multi_line_string;
mod multi_point;
mod multi_polygon;
mod point;
mod polygon;
mod rect;
mod triangle;
