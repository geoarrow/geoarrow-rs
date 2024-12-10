//! Implementations of immutable GeoArrow arrays plus builders to more easily create arrays.

#![allow(missing_docs)] // FIXME

pub use binary::{WKBArray, WKBBuilder, WKBCapacity};
pub use cast::{AsChunkedNativeArray, AsNativeArray, AsSerializedArray};
pub use coord::{
    CoordBuffer, CoordBufferBuilder, CoordType, InterleavedCoordBuffer,
    InterleavedCoordBufferBuilder, SeparatedCoordBuffer, SeparatedCoordBufferBuilder,
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
pub mod metadata;
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
