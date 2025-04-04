//! Implementations of immutable GeoArrow arrays plus builders to more easily create arrays.
//!
//! There are three primary types of structs in this module: arrays, builders, and capacity
//! counters.
//!
//! ## Arrays
//!
//! Arrays
//!
//! These arrays implement the binary layout defined in the [GeoArrow specification](https://github.com/geoarrow/geoarrow).
//!
//!
//!
//! These include:
//!
//! - [`PointArray`]
//! - [`LineStringArray`]
//! - [`PolygonArray`]
//! - [`MultiPointArray`]
//! - [`MultiLineStringArray`]
//! - [`MultiPolygonArray`]
//! - [`GeometryArray`]
//! - [`GeometryCollectionArray`]
//! - [`RectArray`]
//!
//! ## Builders
//!
//! Builders are designed to make it easier
//!
//! There's a builder for each of the above array types:
//!
//!
//! - [`PointBuilder`]
//! - [`LineStringBuilder`]
//! - [`PolygonBuilder`]
//! - [`MultiPointBuilder`]
//! - [`MultiLineStringBuilder`]
//! - [`MultiPolygonBuilder`]
//! - [`GeometryBuilder`]
//! - [`GeometryCollectionBuilder`]
//! - [`RectBuilder`]
//!
//! Once you've finished adding geometries to a builder, it's `O(1)` to convert a builder to an
//! array, by calling `finish()`.
//!
//! ## Capacity Counters
//!
//! Underlying the builders are growable `Vec`s. E.g. you can think of a `PointBuilder` as a buffer of `x` coordinates and a buffer of `y` coordinates.
//!
//! The fastest and most memory-efficient way to construct an array from a set of known geometries
//! is to make a first pass over these geometries to count exactly how big each part of the Arrow
//! array must be, allocate _once_ for exactly what you need, and then fill those buffers in a
//! second pass.
//!

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
