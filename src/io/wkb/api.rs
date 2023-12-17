use std::sync::Arc;

use crate::array::geometrycollection::GeometryCollectionBuilder;
use crate::array::*;
use crate::datatypes::GeoDataType;
use crate::error::Result;
use crate::scalar::WKB;
use crate::GeometryArrayTrait;
use arrow_array::OffsetSizeTrait;

/// Parse a [WKBArray] to a GeometryArray with GeoArrow native encoding.
pub fn from_wkb<O: OffsetSizeTrait>(
    arr: &WKBArray<O>,
    large_type: bool,
    coord_type: CoordType,
) -> Result<Arc<dyn GeometryArrayTrait>> {
    let wkb_objects: Vec<Option<WKB<'_, O>>> = arr.iter().collect();
    if large_type {
        let builder =
            GeometryCollectionBuilder::<i64>::from_wkb(&wkb_objects, Some(coord_type), true)?;
        Ok(builder.finish().downcast())
    } else {
        let builder =
            GeometryCollectionBuilder::<i32>::from_wkb(&wkb_objects, Some(coord_type), true)?;
        Ok(builder.finish().downcast())
    }
}

/// Convert a geometry array to a [WKBArray].
pub fn to_wkb<O: OffsetSizeTrait>(arr: &dyn GeometryArrayTrait) -> WKBArray<O> {
    match arr.data_type() {
        GeoDataType::Point(_) => arr.as_any().downcast_ref::<PointArray>().unwrap().into(),
        GeoDataType::LineString(_) => arr
            .as_any()
            .downcast_ref::<LineStringArray<i32>>()
            .unwrap()
            .into(),
        GeoDataType::LargeLineString(_) => arr
            .as_any()
            .downcast_ref::<LineStringArray<i64>>()
            .unwrap()
            .into(),
        GeoDataType::Polygon(_) => arr
            .as_any()
            .downcast_ref::<PolygonArray<i32>>()
            .unwrap()
            .into(),
        GeoDataType::LargePolygon(_) => arr
            .as_any()
            .downcast_ref::<PolygonArray<i64>>()
            .unwrap()
            .into(),
        GeoDataType::MultiPoint(_) => arr
            .as_any()
            .downcast_ref::<MultiPointArray<i32>>()
            .unwrap()
            .into(),
        GeoDataType::LargeMultiPoint(_) => arr
            .as_any()
            .downcast_ref::<MultiPointArray<i64>>()
            .unwrap()
            .into(),
        GeoDataType::MultiLineString(_) => arr
            .as_any()
            .downcast_ref::<MultiLineStringArray<i32>>()
            .unwrap()
            .into(),
        GeoDataType::LargeMultiLineString(_) => arr
            .as_any()
            .downcast_ref::<MultiLineStringArray<i64>>()
            .unwrap()
            .into(),
        GeoDataType::MultiPolygon(_) => arr
            .as_any()
            .downcast_ref::<MultiPolygonArray<i32>>()
            .unwrap()
            .into(),
        GeoDataType::LargeMultiPolygon(_) => arr
            .as_any()
            .downcast_ref::<MultiPolygonArray<i64>>()
            .unwrap()
            .into(),
        GeoDataType::Mixed(_) => arr
            .as_any()
            .downcast_ref::<MixedGeometryArray<i32>>()
            .unwrap()
            .into(),
        GeoDataType::LargeMixed(_) => arr
            .as_any()
            .downcast_ref::<MixedGeometryArray<i64>>()
            .unwrap()
            .into(),
        GeoDataType::GeometryCollection(_) => todo!(),
        GeoDataType::LargeGeometryCollection(_) => todo!(),
        GeoDataType::WKB => todo!(),
        GeoDataType::LargeWKB => todo!(),
        GeoDataType::Rect => todo!(),
    }
}
