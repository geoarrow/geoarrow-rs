//! Implementations of immutable GeoArrow arrays plus builders to more easily create arrays.

#![allow(missing_docs)] // FIXME

pub use binary::{WKBArray, WKBBuilder, WKBCapacity};
pub use cast::{AsChunkedNativeArray, AsNativeArray};
pub use coord::{
    CoordBuffer, CoordBufferBuilder, CoordType, InterleavedCoordBuffer,
    InterleavedCoordBufferBuilder, SeparatedCoordBuffer, SeparatedCoordBufferBuilder,
};
pub use dynamic::NativeArrayDyn;
pub use geometrycollection::{
    GeometryCollectionArray, GeometryCollectionBuilder, GeometryCollectionCapacity,
};
pub use linestring::{LineStringArray, LineStringBuilder, LineStringCapacity};
pub use mixed::{MixedCapacity, MixedGeometryArray, MixedGeometryBuilder};
pub use multilinestring::{MultiLineStringArray, MultiLineStringBuilder, MultiLineStringCapacity};
pub use multipoint::{MultiPointArray, MultiPointBuilder, MultiPointCapacity};
pub use multipolygon::{MultiPolygonArray, MultiPolygonBuilder, MultiPolygonCapacity};
pub use point::{PointArray, PointBuilder};
pub use polygon::{PolygonArray, PolygonBuilder, PolygonCapacity};
pub use rect::{RectArray, RectBuilder};

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

use std::sync::Arc;

use arrow_array::Array;
use arrow_schema::Field;

use crate::datatypes::{Dimension, GeoDataType};
use crate::error::Result;
use crate::NativeArray;

/// Convert an Arrow [Array] to a geoarrow GeometryArray
pub fn from_arrow_array(array: &dyn Array, field: &Field) -> Result<Arc<dyn NativeArray>> {
    let data_type = GeoDataType::try_from(field)?;

    use Dimension::*;
    use GeoDataType::*;
    let geo_arr: Arc<dyn NativeArray> = match data_type {
        Point(_, dim) => match dim {
            XY => Arc::new(PointArray::<2>::try_from((array, field))?),
            XYZ => Arc::new(PointArray::<3>::try_from((array, field))?),
        },
        LineString(_, dim) => match dim {
            XY => Arc::new(LineStringArray::<i32, 2>::try_from((array, field))?),
            XYZ => Arc::new(LineStringArray::<i32, 3>::try_from((array, field))?),
        },
        LargeLineString(_, dim) => match dim {
            XY => Arc::new(LineStringArray::<i64, 2>::try_from((array, field))?),
            XYZ => Arc::new(LineStringArray::<i64, 3>::try_from((array, field))?),
        },
        Polygon(_, dim) => match dim {
            XY => Arc::new(PolygonArray::<i32, 2>::try_from((array, field))?),
            XYZ => Arc::new(PolygonArray::<i32, 3>::try_from((array, field))?),
        },
        LargePolygon(_, dim) => match dim {
            XY => Arc::new(PolygonArray::<i64, 2>::try_from((array, field))?),
            XYZ => Arc::new(PolygonArray::<i64, 3>::try_from((array, field))?),
        },
        MultiPoint(_, dim) => match dim {
            XY => Arc::new(MultiPointArray::<i32, 2>::try_from((array, field))?),
            XYZ => Arc::new(MultiPointArray::<i32, 3>::try_from((array, field))?),
        },
        LargeMultiPoint(_, dim) => match dim {
            XY => Arc::new(MultiPointArray::<i64, 2>::try_from((array, field))?),
            XYZ => Arc::new(MultiPointArray::<i64, 3>::try_from((array, field))?),
        },
        MultiLineString(_, dim) => match dim {
            XY => Arc::new(MultiLineStringArray::<i32, 2>::try_from((array, field))?),
            XYZ => Arc::new(MultiLineStringArray::<i32, 3>::try_from((array, field))?),
        },
        LargeMultiLineString(_, dim) => match dim {
            XY => Arc::new(MultiLineStringArray::<i64, 2>::try_from((array, field))?),
            XYZ => Arc::new(MultiLineStringArray::<i64, 3>::try_from((array, field))?),
        },
        MultiPolygon(_, dim) => match dim {
            XY => Arc::new(MultiPolygonArray::<i32, 2>::try_from((array, field))?),
            XYZ => Arc::new(MultiPolygonArray::<i32, 3>::try_from((array, field))?),
        },
        LargeMultiPolygon(_, dim) => match dim {
            XY => Arc::new(MultiPolygonArray::<i64, 2>::try_from((array, field))?),
            XYZ => Arc::new(MultiPolygonArray::<i64, 3>::try_from((array, field))?),
        },
        Mixed(_, dim) => match dim {
            XY => Arc::new(MixedGeometryArray::<i32, 2>::try_from((array, field))?),
            XYZ => Arc::new(MixedGeometryArray::<i32, 3>::try_from((array, field))?),
        },
        LargeMixed(_, dim) => match dim {
            XY => Arc::new(MixedGeometryArray::<i64, 2>::try_from((array, field))?),
            XYZ => Arc::new(MixedGeometryArray::<i64, 3>::try_from((array, field))?),
        },
        GeometryCollection(_, dim) => match dim {
            XY => Arc::new(GeometryCollectionArray::<i32, 2>::try_from((array, field))?),
            XYZ => Arc::new(GeometryCollectionArray::<i32, 3>::try_from((array, field))?),
        },
        LargeGeometryCollection(_, dim) => match dim {
            XY => Arc::new(GeometryCollectionArray::<i64, 2>::try_from((array, field))?),
            XYZ => Arc::new(GeometryCollectionArray::<i64, 3>::try_from((array, field))?),
        },
        WKB => Arc::new(WKBArray::<i32>::try_from((array, field))?),
        LargeWKB => Arc::new(WKBArray::<i64>::try_from((array, field))?),
        Rect(dim) => match dim {
            XY => Arc::new(RectArray::<2>::try_from((array, field))?),
            XYZ => Arc::new(RectArray::<3>::try_from((array, field))?),
        },
    };

    Ok(geo_arr)
}
