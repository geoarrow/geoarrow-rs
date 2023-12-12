use std::sync::Arc;

use crate::array::mixed::MixedCapacity;
use crate::array::*;
use crate::datatypes::GeoDataType;
use crate::error::Result;
use crate::io::wkb::reader::geometry::WKBGeometry;
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
    let wkb_objects2: Vec<Option<WKBGeometry>> = wkb_objects
        .iter()
        .map(|maybe_wkb| maybe_wkb.as_ref().map(|wkb| wkb.to_wkb_object()))
        .collect();

    let capacity = MixedCapacity::from_owned_geometries(wkb_objects2.into_iter());
    if capacity.point_compatible() {
        let mut builder =
            PointBuilder::with_capacity_and_options(capacity.point_capacity(), coord_type);
        let wkb_points: Vec<Option<_>> = wkb_objects
            .iter()
            .map(|maybe_wkb| {
                maybe_wkb
                    .as_ref()
                    .map(|wkb| wkb.to_wkb_object().into_point())
            })
            .collect();
        builder.extend_from_iter(wkb_points.iter().map(|x| x.as_ref()));
        Ok(Arc::new(builder.finish()))
    } else if capacity.line_string_compatible() {
        let wkb_line_strings: Vec<Option<_>> = wkb_objects
            .iter()
            .map(|maybe_wkb| {
                maybe_wkb
                    .as_ref()
                    .map(|wkb| wkb.to_wkb_object().into_line_string())
            })
            .collect();
        if large_type {
            let mut builder = LineStringBuilder::<i64>::with_capacity_and_options(
                capacity.line_string_capacity(),
                coord_type,
            );
            builder.extend_from_iter(wkb_line_strings.iter().map(|x| x.as_ref()));
            Ok(Arc::new(builder.finish()))
        } else {
            let mut builder = LineStringBuilder::<i32>::with_capacity_and_options(
                capacity.line_string_capacity(),
                coord_type,
            );
            builder.extend_from_iter(wkb_line_strings.iter().map(|x| x.as_ref()));
            Ok(Arc::new(builder.finish()))
        }
    } else if capacity.polygon_compatible() {
        let wkb_polygons: Vec<Option<_>> = wkb_objects
            .iter()
            .map(|maybe_wkb| {
                maybe_wkb
                    .as_ref()
                    .map(|wkb| wkb.to_wkb_object().into_polygon())
            })
            .collect();
        if large_type {
            let mut builder = PolygonBuilder::<i64>::with_capacity_and_options(
                capacity.polygon_capacity(),
                coord_type,
            );
            builder.extend_from_iter(wkb_polygons.iter().map(|x| x.as_ref()));
            Ok(Arc::new(builder.finish()))
        } else {
            let mut builder = PolygonBuilder::<i32>::with_capacity_and_options(
                capacity.polygon_capacity(),
                coord_type,
            );
            builder.extend_from_iter(wkb_polygons.iter().map(|x| x.as_ref()));
            Ok(Arc::new(builder.finish()))
        }
    } else if capacity.multi_point_compatible() {
        let wkb_multi_points: Vec<Option<_>> = wkb_objects
            .iter()
            .map(|maybe_wkb| {
                maybe_wkb
                    .as_ref()
                    .map(|wkb| wkb.to_wkb_object().into_maybe_multi_point())
            })
            .collect();

        // Have to add point and multi point capacity together
        let mut multi_point_capacity = capacity.multi_point_capacity();
        multi_point_capacity.add_point_capacity(capacity.point_capacity());

        if large_type {
            let mut builder = MultiPointBuilder::<i64>::with_capacity_and_options(
                multi_point_capacity,
                coord_type,
            );
            builder.extend_from_iter(wkb_multi_points.iter().map(|x| x.as_ref()));
            Ok(Arc::new(builder.finish()))
        } else {
            let mut builder = MultiPointBuilder::<i32>::with_capacity_and_options(
                multi_point_capacity,
                coord_type,
            );
            builder.extend_from_iter(wkb_multi_points.iter().map(|x| x.as_ref()));
            Ok(Arc::new(builder.finish()))
        }
    } else if capacity.multi_line_string_compatible() {
        let wkb_multi_line_strings: Vec<Option<_>> = wkb_objects
            .iter()
            .map(|maybe_wkb| {
                maybe_wkb
                    .as_ref()
                    .map(|wkb| wkb.to_wkb_object().into_maybe_multi_line_string())
            })
            .collect();

        // Have to add line string and multi line string capacity together
        let mut multi_line_string_capacity = capacity.multi_line_string_capacity();
        multi_line_string_capacity.add_line_string_capacity(capacity.line_string_capacity());

        if large_type {
            let mut builder = MultiLineStringBuilder::<i64>::with_capacity_and_options(
                multi_line_string_capacity,
                coord_type,
            );
            builder.extend_from_iter(wkb_multi_line_strings.iter().map(|x| x.as_ref()));
            Ok(Arc::new(builder.finish()))
        } else {
            let mut builder = MultiLineStringBuilder::<i32>::with_capacity_and_options(
                multi_line_string_capacity,
                coord_type,
            );
            builder.extend_from_iter(wkb_multi_line_strings.iter().map(|x| x.as_ref()));
            Ok(Arc::new(builder.finish()))
        }
    } else if capacity.multi_polygon_compatible() {
        let wkb_multi_polygons: Vec<Option<_>> = wkb_objects
            .iter()
            .map(|maybe_wkb| {
                maybe_wkb
                    .as_ref()
                    .map(|wkb| wkb.to_wkb_object().into_maybe_multi_polygon())
            })
            .collect();

        // Have to add line string and multi line string capacity together
        let mut multi_polygon_capacity = capacity.multi_polygon_capacity();
        multi_polygon_capacity.add_polygon_capacity(capacity.polygon_capacity());

        if large_type {
            let mut builder = MultiPolygonBuilder::<i64>::with_capacity_and_options(
                multi_polygon_capacity,
                coord_type,
            );
            builder.extend_from_iter(wkb_multi_polygons.iter().map(|x| x.as_ref()));
            Ok(Arc::new(builder.finish()))
        } else {
            let mut builder = MultiPolygonBuilder::<i32>::with_capacity_and_options(
                multi_polygon_capacity,
                coord_type,
            );
            builder.extend_from_iter(wkb_multi_polygons.iter().map(|x| x.as_ref()));
            Ok(Arc::new(builder.finish()))
        }
    } else {
        let wkb_geometry: Vec<Option<_>> = wkb_objects
            .iter()
            .map(|maybe_wkb| maybe_wkb.as_ref().map(|wkb| wkb.to_wkb_object()))
            .collect();

        #[allow(clippy::collapsible_else_if)]
        if large_type {
            let mut builder =
                MixedGeometryBuilder::<i64>::with_capacity_and_options(capacity, coord_type);
            builder.extend_from_iter(wkb_geometry.iter().map(|x| x.as_ref()), true);
            Ok(Arc::new(builder.finish()))
        } else {
            let mut builder =
                MixedGeometryBuilder::<i32>::with_capacity_and_options(capacity, coord_type);
            builder.extend_from_iter(wkb_geometry.iter().map(|x| x.as_ref()), true);
            Ok(Arc::new(builder.finish()))
        }
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
