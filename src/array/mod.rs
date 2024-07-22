//! Implementations of immutable GeoArrow arrays plus builders to more easily create arrays.

pub use binary::{WKBArray, WKBBuilder, WKBCapacity};
pub use cast::{AsChunkedGeometryArray, AsGeometryArray};
pub use coord::{
    CoordBuffer, CoordBufferBuilder, CoordType, InterleavedCoordBuffer,
    InterleavedCoordBufferBuilder, SeparatedCoordBuffer, SeparatedCoordBufferBuilder,
};
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
use crate::GeometryArrayTrait;

/// Convert an Arrow [Array] to a geoarrow GeometryArray
pub fn from_arrow_array(array: &dyn Array, field: &Field) -> Result<Arc<dyn GeometryArrayTrait>> {
    let data_type = GeoDataType::try_from(field)?;
    let geo_arr: Arc<dyn GeometryArrayTrait> = match data_type {
        GeoDataType::Point(_, dim) => match dim {
            Dimension::XY => Arc::new(PointArray::<2>::try_from((array, field))?),
            Dimension::XYZ => Arc::new(PointArray::<3>::try_from((array, field))?),
        },
        GeoDataType::LineString(_, dim) => match dim {
            Dimension::XY => Arc::new(LineStringArray::<i32, 2>::try_from((array, field))?),
            Dimension::XYZ => Arc::new(LineStringArray::<i32, 3>::try_from((array, field))?),
        },
        GeoDataType::LargeLineString(_, dim) => match dim {
            Dimension::XY => Arc::new(LineStringArray::<i64, 2>::try_from((array, field))?),
            Dimension::XYZ => Arc::new(LineStringArray::<i64, 3>::try_from((array, field))?),
        },
        GeoDataType::Polygon(_, dim) => match dim {
            Dimension::XY => Arc::new(PolygonArray::<i32, 2>::try_from((array, field))?),
            Dimension::XYZ => Arc::new(PolygonArray::<i32, 3>::try_from((array, field))?),
        },
        GeoDataType::LargePolygon(_, dim) => match dim {
            Dimension::XY => Arc::new(PolygonArray::<i64, 2>::try_from((array, field))?),
            Dimension::XYZ => Arc::new(PolygonArray::<i64, 3>::try_from((array, field))?),
        },
        GeoDataType::MultiPoint(_, dim) => match dim {
            Dimension::XY => Arc::new(MultiPointArray::<i32, 2>::try_from((array, field))?),
            Dimension::XYZ => Arc::new(MultiPointArray::<i32, 3>::try_from((array, field))?),
        },
        GeoDataType::LargeMultiPoint(_, dim) => match dim {
            Dimension::XY => Arc::new(MultiPointArray::<i64, 2>::try_from((array, field))?),
            Dimension::XYZ => Arc::new(MultiPointArray::<i64, 3>::try_from((array, field))?),
        },
        GeoDataType::MultiLineString(_, dim) => match dim {
            Dimension::XY => Arc::new(MultiLineStringArray::<i32, 2>::try_from((array, field))?),
            Dimension::XYZ => Arc::new(MultiLineStringArray::<i32, 3>::try_from((array, field))?),
        },
        GeoDataType::LargeMultiLineString(_, dim) => match dim {
            Dimension::XY => Arc::new(MultiLineStringArray::<i64, 2>::try_from((array, field))?),
            Dimension::XYZ => Arc::new(MultiLineStringArray::<i64, 3>::try_from((array, field))?),
        },
        GeoDataType::MultiPolygon(_, dim) => match dim {
            Dimension::XY => Arc::new(MultiPolygonArray::<i32, 2>::try_from((array, field))?),
            Dimension::XYZ => Arc::new(MultiPolygonArray::<i32, 3>::try_from((array, field))?),
        },
        GeoDataType::LargeMultiPolygon(_, dim) => match dim {
            Dimension::XY => Arc::new(MultiPolygonArray::<i64, 2>::try_from((array, field))?),
            Dimension::XYZ => Arc::new(MultiPolygonArray::<i64, 3>::try_from((array, field))?),
        },
        GeoDataType::Mixed(_, dim) => match dim {
            Dimension::XY => Arc::new(MixedGeometryArray::<i32, 2>::try_from((array, field))?),
            Dimension::XYZ => Arc::new(MixedGeometryArray::<i32, 3>::try_from((array, field))?),
        },
        GeoDataType::LargeMixed(_, dim) => match dim {
            Dimension::XY => Arc::new(MixedGeometryArray::<i64, 2>::try_from((array, field))?),
            Dimension::XYZ => Arc::new(MixedGeometryArray::<i64, 3>::try_from((array, field))?),
        },
        GeoDataType::GeometryCollection(_, dim) => match dim {
            Dimension::XY => Arc::new(GeometryCollectionArray::<i32, 2>::try_from((array, field))?),
            Dimension::XYZ => {
                Arc::new(GeometryCollectionArray::<i32, 3>::try_from((array, field))?)
            }
        },
        GeoDataType::LargeGeometryCollection(_, dim) => match dim {
            Dimension::XY => Arc::new(GeometryCollectionArray::<i64, 2>::try_from((array, field))?),
            Dimension::XYZ => {
                Arc::new(GeometryCollectionArray::<i64, 3>::try_from((array, field))?)
            }
        },
        GeoDataType::WKB => Arc::new(WKBArray::<i32>::try_from((array, field))?),
        GeoDataType::LargeWKB => Arc::new(WKBArray::<i64>::try_from((array, field))?),
        GeoDataType::Rect => todo!("construct rect array from ArrayRef"),
    };

    Ok(geo_arr)
}
