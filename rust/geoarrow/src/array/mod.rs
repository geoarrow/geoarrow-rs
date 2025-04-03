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

pub use binary::{WKBArray, WKBBuilder, WKBCapacity};
pub use cast::{AsChunkedNativeArray, AsNativeArray, AsSerializedArray};
pub use coord::{
    CoordBuffer, CoordBufferBuilder, InterleavedCoordBuffer, InterleavedCoordBufferBuilder,
    SeparatedCoordBuffer, SeparatedCoordBufferBuilder,
};
pub use dynamic::{NativeArrayDyn, SerializedArrayDyn};
pub use geometry::{GeometryArray, GeometryBuilder, GeometryCapacity};
pub use geometrycollection::{
    GeometryCollectionArray, GeometryCollectionBuilder, GeometryCollectionCapacity,
};
pub use linestring::{LineStringArray, LineStringBuilder, LineStringCapacity};
// Don't expose in the public API. Prefer GeometryArray
pub(crate) use mixed::{MixedCapacity, MixedGeometryArray, MixedGeometryBuilder};
pub use multilinestring::{MultiLineStringArray, MultiLineStringBuilder, MultiLineStringCapacity};
pub use multipoint::{MultiPointArray, MultiPointBuilder, MultiPointCapacity};
pub use multipolygon::{MultiPolygonArray, MultiPolygonBuilder, MultiPolygonCapacity};
pub use point::{PointArray, PointBuilder};
pub use polygon::{PolygonArray, PolygonBuilder, PolygonCapacity};
pub use rect::{RectArray, RectBuilder};
pub use wkt::WKTArray;

pub use crate::trait_::{ArrayBase, NativeArray, SerializedArray};

pub(crate) mod binary;
mod cast;
pub(crate) mod coord;
pub(crate) mod dynamic;
pub(crate) mod geometry;
pub(crate) mod geometrycollection;
pub(crate) mod linestring;
pub(crate) mod mixed;
pub(crate) mod multilinestring;
pub(crate) mod multipoint;
pub(crate) mod multipolygon;
pub(crate) mod offset_builder;
pub(crate) mod point;
pub(crate) mod polygon;
pub(crate) mod rect;
pub(crate) mod util;
pub(crate) mod wkt;

use std::sync::Arc;

use arrow_array::Array;
use arrow_schema::Field;

use crate::error::Result;

/// Convert an Arrow [Array] to a geoarrow GeometryArray
#[deprecated = "Use NativeArrayDyn::from_arrow_array instead."]
pub fn from_arrow_array(array: &dyn Array, field: &Field) -> Result<Arc<dyn NativeArray>> {
    Ok(NativeArrayDyn::from_arrow_array(array, field)?.into_inner())
}
