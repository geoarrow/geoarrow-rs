#![allow(unused_variables, dead_code)]

use std::collections::HashSet;
use std::sync::Arc;

use arrow_array::{OffsetSizeTrait, RecordBatch};
use arrow_buffer::OffsetBuffer;
use arrow_schema::Schema;

use crate::algorithm::native::cast::Cast;
use crate::array::offset_builder::OffsetsBuilder;
use crate::array::util::OffsetBufferUtils;
use crate::array::*;
use crate::chunked_array::*;
use crate::datatypes::GeoDataType;
use crate::error::Result;
use crate::table::GeoTable;
use crate::GeometryArrayTrait;

pub trait Downcast {
    type Output;

    /// The data type that downcasting would result in.
    fn downcasted_data_type(&self, small_offsets: bool) -> GeoDataType;

    /// If possible, convert this array to a simpler and/or smaller data type
    ///
    /// Conversions include:
    ///
    /// - MultiPoint -> Point
    /// - MultiLineString -> LineString
    /// - MultiPolygon -> Polygon
    /// - MixedGeometry -> any of the 6 concrete types
    /// - GeometryCollection -> MixedGeometry or any of the 6 concrete types
    ///
    /// If small_offsets is `true`, it will additionally try to convert `i64` offset buffers to
    /// `i32` if the offsets would not overflow.
    fn downcast(&self, small_offsets: bool) -> Self::Output;
}

impl Downcast for PointArray {
    type Output = Arc<dyn GeometryArrayTrait>;

    fn downcasted_data_type(&self, small_offsets: bool) -> GeoDataType {
        *self.data_type()
    }

    fn downcast(&self, small_offsets: bool) -> Self::Output {
        Arc::new(self.clone())
    }
}

/// Returns `true` if this offsets buffer is type `i64` and would fit in an `i32`
///
/// If the offset type `O` is already `i32`, will return false
fn can_downcast_offsets_i32<O: OffsetSizeTrait>(buffer: &OffsetBuffer<O>) -> bool {
    if O::IS_LARGE {
        buffer.last().to_usize().unwrap() < i32::MAX as usize
    } else {
        false
    }
}

/// Downcast an i64 offset buffer to i32
///
/// This copies the buffer into an i32
fn downcast_offsets<O: OffsetSizeTrait>(buffer: &OffsetBuffer<O>) -> OffsetBuffer<i32> {
    if O::IS_LARGE {
        let mut builder = OffsetsBuilder::with_capacity(buffer.len_proxy());
        buffer
            .iter()
            .for_each(|x| builder.try_push(x.to_usize().unwrap() as i32).unwrap());
        builder.finish()
    } else {
        // This function should never be called when offsets are i32
        unreachable!()
    }
}

/// Returns `true` if this Multi-geometry array can fit into a non-multi array
///
/// Note that we can't just check the value of the last offset, because there could be a null
/// element with length 0 and then a multi point of length 2. We need to check that every offset is
/// <= 1.
fn can_downcast_multi<O: OffsetSizeTrait>(buffer: &OffsetBuffer<O>) -> bool {
    buffer
        .windows(2)
        .all(|slice| *slice.get(1).unwrap() - *slice.first().unwrap() <= O::one())
}

impl<O: OffsetSizeTrait> Downcast for LineStringArray<O> {
    type Output = Arc<dyn GeometryArrayTrait>;

    fn downcasted_data_type(&self, small_offsets: bool) -> GeoDataType {
        match self.data_type() {
            GeoDataType::LineString(ct) => GeoDataType::LineString(*ct),
            GeoDataType::LargeLineString(ct) => {
                if small_offsets && can_downcast_offsets_i32(&self.geom_offsets) {
                    GeoDataType::LineString(*ct)
                } else {
                    GeoDataType::LargeLineString(*ct)
                }
            }
            _ => unreachable!(),
        }
    }

    fn downcast(&self, small_offsets: bool) -> Self::Output {
        match (*self.data_type(), self.downcasted_data_type(small_offsets)) {
            (GeoDataType::LineString(_), GeoDataType::LineString(_))
            | (GeoDataType::LargeLineString(_), GeoDataType::LargeLineString(_)) => {
                Arc::new(self.clone())
            }
            (GeoDataType::LargeLineString(_), GeoDataType::LineString(_)) => todo!(),
            _ => unreachable!(),
        }
    }
}

impl<O: OffsetSizeTrait> Downcast for PolygonArray<O> {
    type Output = Arc<dyn GeometryArrayTrait>;

    fn downcasted_data_type(&self, small_offsets: bool) -> GeoDataType {
        match self.data_type() {
            GeoDataType::Polygon(ct) => GeoDataType::Polygon(*ct),
            GeoDataType::LargePolygon(ct) => {
                if small_offsets && can_downcast_offsets_i32(&self.ring_offsets) {
                    GeoDataType::Polygon(*ct)
                } else {
                    GeoDataType::LargePolygon(*ct)
                }
            }
            _ => unreachable!(),
        }
    }

    fn downcast(&self, small_offsets: bool) -> Self::Output {
        Arc::new(self.clone())
    }
}

impl<O: OffsetSizeTrait> Downcast for MultiPointArray<O> {
    type Output = Arc<dyn GeometryArrayTrait>;

    fn downcasted_data_type(&self, small_offsets: bool) -> GeoDataType {
        match self.data_type() {
            GeoDataType::MultiPoint(ct) => {
                if can_downcast_multi(&self.geom_offsets) {
                    GeoDataType::Point(*ct)
                } else {
                    GeoDataType::MultiPoint(*ct)
                }
            }
            GeoDataType::LargeMultiPoint(ct) => {
                match (
                    can_downcast_multi(&self.geom_offsets),
                    small_offsets && can_downcast_offsets_i32(&self.geom_offsets),
                ) {
                    (true, _) => GeoDataType::Point(*ct),
                    (false, true) => GeoDataType::MultiPoint(*ct),
                    (false, false) => GeoDataType::LargeMultiPoint(*ct),
                }
            }
            _ => unreachable!(),
        }
    }
    fn downcast(&self, small_offsets: bool) -> Self::Output {
        // Note: this won't allow a downcast for empty MultiPoints
        if self.geom_offsets.last().to_usize().unwrap() == self.len() {
            return Arc::new(PointArray::new(
                self.coords.clone(),
                self.validity.clone(),
                self.metadata(),
            ));
        }

        Arc::new(self.clone())
    }
}

impl<O: OffsetSizeTrait> Downcast for MultiLineStringArray<O> {
    type Output = Arc<dyn GeometryArrayTrait>;

    fn downcasted_data_type(&self, small_offsets: bool) -> GeoDataType {
        match self.data_type() {
            GeoDataType::MultiLineString(ct) => {
                if can_downcast_multi(&self.geom_offsets) {
                    GeoDataType::LineString(*ct)
                } else {
                    GeoDataType::MultiLineString(*ct)
                }
            }
            GeoDataType::LargeMultiLineString(ct) => {
                match (
                    can_downcast_multi(&self.geom_offsets),
                    small_offsets && can_downcast_offsets_i32(&self.ring_offsets),
                ) {
                    (true, true) => GeoDataType::LineString(*ct),
                    (true, false) => GeoDataType::LargeLineString(*ct),
                    (false, true) => GeoDataType::MultiLineString(*ct),
                    (false, false) => GeoDataType::LargeMultiLineString(*ct),
                }
            }
            _ => unreachable!(),
        }
    }

    fn downcast(&self, small_offsets: bool) -> Self::Output {
        if self.geom_offsets.last().to_usize().unwrap() == self.len() {
            return Arc::new(LineStringArray::new(
                self.coords.clone(),
                self.ring_offsets.clone(),
                self.validity.clone(),
                self.metadata(),
            ));
        }

        Arc::new(self.clone())
    }
}

impl<O: OffsetSizeTrait> Downcast for MultiPolygonArray<O> {
    type Output = Arc<dyn GeometryArrayTrait>;

    fn downcasted_data_type(&self, small_offsets: bool) -> GeoDataType {
        match self.data_type() {
            GeoDataType::MultiPolygon(ct) => {
                if can_downcast_multi(&self.geom_offsets) {
                    GeoDataType::Polygon(*ct)
                } else {
                    GeoDataType::MultiPolygon(*ct)
                }
            }
            GeoDataType::LargeMultiPolygon(ct) => {
                match (
                    can_downcast_multi(&self.geom_offsets),
                    small_offsets && can_downcast_offsets_i32(&self.ring_offsets),
                ) {
                    (true, true) => GeoDataType::Polygon(*ct),
                    (true, false) => GeoDataType::LargePolygon(*ct),
                    (false, true) => GeoDataType::MultiPolygon(*ct),
                    (false, false) => GeoDataType::LargeMultiPolygon(*ct),
                }
            }
            _ => unreachable!(),
        }
    }

    fn downcast(&self, small_offsets: bool) -> Self::Output {
        if self.geom_offsets.last().to_usize().unwrap() == self.len() {
            return Arc::new(PolygonArray::new(
                self.coords.clone(),
                self.polygon_offsets.clone(),
                self.ring_offsets.clone(),
                self.validity.clone(),
                self.metadata(),
            ));
        }

        Arc::new(self.clone())
    }
}

impl<O: OffsetSizeTrait> Downcast for MixedGeometryArray<O> {
    type Output = Arc<dyn GeometryArrayTrait>;

    fn downcasted_data_type(&self, small_offsets: bool) -> GeoDataType {
        let coord_type = self.coord_type();

        if self.points.is_some()
            && self.line_strings.is_none()
            && self.polygons.is_none()
            && self.multi_points.is_none()
            && self.multi_line_strings.is_none()
            && self.multi_polygons.is_none()
        {
            return GeoDataType::Point(coord_type);
        }

        if self.points.is_none()
            && self.line_strings.is_some()
            && self.polygons.is_none()
            && self.multi_points.is_none()
            && self.multi_line_strings.is_none()
            && self.multi_polygons.is_none()
        {
            return self
                .line_strings
                .as_ref()
                .unwrap()
                .downcasted_data_type(small_offsets);
        }

        if self.points.is_none()
            && self.line_strings.is_none()
            && self.polygons.is_some()
            && self.multi_points.is_none()
            && self.multi_line_strings.is_none()
            && self.multi_polygons.is_none()
        {
            return self
                .polygons
                .as_ref()
                .unwrap()
                .downcasted_data_type(small_offsets);
        }

        if self.points.is_none()
            && self.line_strings.is_none()
            && self.polygons.is_none()
            && self.multi_points.is_some()
            && self.multi_line_strings.is_none()
            && self.multi_polygons.is_none()
        {
            return self
                .multi_points
                .as_ref()
                .unwrap()
                .downcasted_data_type(small_offsets);
        }

        if self.points.is_none()
            && self.line_strings.is_none()
            && self.polygons.is_none()
            && self.multi_points.is_none()
            && self.multi_line_strings.is_some()
            && self.multi_polygons.is_none()
        {
            return self
                .multi_line_strings
                .as_ref()
                .unwrap()
                .downcasted_data_type(small_offsets);
        }

        if self.points.is_none()
            && self.line_strings.is_none()
            && self.polygons.is_none()
            && self.multi_points.is_none()
            && self.multi_line_strings.is_none()
            && self.multi_polygons.is_some()
        {
            return self
                .multi_polygons
                .as_ref()
                .unwrap()
                .downcasted_data_type(small_offsets);
        }

        *self.data_type()
    }

    fn downcast(&self, small_offsets: bool) -> Self::Output {
        // TODO: do I need to handle the slice offset?
        if self.points.is_some()
            && self.line_strings.is_none()
            && self.polygons.is_none()
            && self.multi_points.is_none()
            && self.multi_line_strings.is_none()
            && self.multi_polygons.is_none()
        {
            return Arc::new(self.points.as_ref().unwrap().clone());
        }

        if self.points.is_none()
            && self.line_strings.is_some()
            && self.polygons.is_none()
            && self.multi_points.is_none()
            && self.multi_line_strings.is_none()
            && self.multi_polygons.is_none()
        {
            return self.line_strings.as_ref().unwrap().downcast(small_offsets);
        }

        if self.points.is_none()
            && self.line_strings.is_none()
            && self.polygons.is_some()
            && self.multi_points.is_none()
            && self.multi_line_strings.is_none()
            && self.multi_polygons.is_none()
        {
            return self.polygons.as_ref().unwrap().downcast(small_offsets);
        }

        if self.points.is_none()
            && self.line_strings.is_none()
            && self.polygons.is_none()
            && self.multi_points.is_some()
            && self.multi_line_strings.is_none()
            && self.multi_polygons.is_none()
        {
            return self.multi_points.as_ref().unwrap().downcast(small_offsets);
        }

        if self.points.is_none()
            && self.line_strings.is_none()
            && self.polygons.is_none()
            && self.multi_points.is_none()
            && self.multi_line_strings.is_some()
            && self.multi_polygons.is_none()
        {
            return self
                .multi_line_strings
                .as_ref()
                .unwrap()
                .downcast(small_offsets);
        }

        if self.points.is_none()
            && self.line_strings.is_none()
            && self.polygons.is_none()
            && self.multi_points.is_none()
            && self.multi_line_strings.is_none()
            && self.multi_polygons.is_some()
        {
            return self
                .multi_polygons
                .as_ref()
                .unwrap()
                .downcast(small_offsets);
        }

        Arc::new(self.clone())
    }
}

impl<O: OffsetSizeTrait> Downcast for GeometryCollectionArray<O> {
    type Output = Arc<dyn GeometryArrayTrait>;

    fn downcasted_data_type(&self, small_offsets: bool) -> GeoDataType {
        todo!()
    }
    fn downcast(&self, small_offsets: bool) -> Self::Output {
        // TODO: support downcasting with null elements
        if self.geom_offsets.last().to_usize().unwrap() == self.len() && self.null_count() == 0 {
            // Call downcast on the mixed array
            return self.array.downcast(small_offsets);
        }

        Arc::new(self.clone())
    }
}

impl Downcast for RectArray {
    type Output = Arc<dyn GeometryArrayTrait>;

    fn downcasted_data_type(&self, small_offsets: bool) -> GeoDataType {
        *self.data_type()
    }
    fn downcast(&self, small_offsets: bool) -> Self::Output {
        Arc::new(self.clone())
    }
}

impl Downcast for &dyn GeometryArrayTrait {
    type Output = Arc<dyn GeometryArrayTrait>;

    fn downcasted_data_type(&self, small_offsets: bool) -> GeoDataType {
        match self.data_type() {
            GeoDataType::Point(_) => self.as_point().downcasted_data_type(small_offsets),
            GeoDataType::LineString(_) => self.as_line_string().downcasted_data_type(small_offsets),
            GeoDataType::LargeLineString(_) => self
                .as_large_line_string()
                .downcasted_data_type(small_offsets),
            GeoDataType::Polygon(_) => self.as_polygon().downcasted_data_type(small_offsets),
            GeoDataType::LargePolygon(_) => {
                self.as_large_polygon().downcasted_data_type(small_offsets)
            }
            GeoDataType::MultiPoint(_) => self.as_multi_point().downcasted_data_type(small_offsets),
            GeoDataType::LargeMultiPoint(_) => self
                .as_large_multi_point()
                .downcasted_data_type(small_offsets),
            GeoDataType::MultiLineString(_) => self
                .as_multi_line_string()
                .downcasted_data_type(small_offsets),
            GeoDataType::LargeMultiLineString(_) => self
                .as_large_multi_line_string()
                .downcasted_data_type(small_offsets),
            GeoDataType::MultiPolygon(_) => {
                self.as_multi_polygon().downcasted_data_type(small_offsets)
            }
            GeoDataType::LargeMultiPolygon(_) => self
                .as_large_multi_polygon()
                .downcasted_data_type(small_offsets),
            GeoDataType::Mixed(_) => self.as_mixed().downcasted_data_type(small_offsets),
            GeoDataType::LargeMixed(_) => self.as_large_mixed().downcasted_data_type(small_offsets),
            GeoDataType::GeometryCollection(_) => self
                .as_geometry_collection()
                .downcasted_data_type(small_offsets),
            GeoDataType::LargeGeometryCollection(_) => self
                .as_large_geometry_collection()
                .downcasted_data_type(small_offsets),
            GeoDataType::Rect => self.as_rect().downcasted_data_type(small_offsets),
            // TODO: downcast largewkb to wkb
            GeoDataType::WKB => *self.data_type(),
            GeoDataType::LargeWKB => *self.data_type(),
        }
    }

    fn downcast(&self, small_offsets: bool) -> Self::Output {
        match self.data_type() {
            GeoDataType::Point(_) => self.as_point().downcast(small_offsets),
            GeoDataType::LineString(_) => self.as_line_string().downcast(small_offsets),
            GeoDataType::LargeLineString(_) => self.as_large_line_string().downcast(small_offsets),
            GeoDataType::Polygon(_) => self.as_polygon().downcast(small_offsets),
            GeoDataType::LargePolygon(_) => self.as_large_polygon().downcast(small_offsets),
            GeoDataType::MultiPoint(_) => self.as_multi_point().downcast(small_offsets),
            GeoDataType::LargeMultiPoint(_) => self.as_large_multi_point().downcast(small_offsets),
            GeoDataType::MultiLineString(_) => self.as_multi_line_string().downcast(small_offsets),
            GeoDataType::LargeMultiLineString(_) => {
                self.as_large_multi_line_string().downcast(small_offsets)
            }
            GeoDataType::MultiPolygon(_) => self.as_multi_polygon().downcast(small_offsets),
            GeoDataType::LargeMultiPolygon(_) => {
                self.as_large_multi_polygon().downcast(small_offsets)
            }
            GeoDataType::Mixed(_) => self.as_mixed().downcast(small_offsets),
            GeoDataType::LargeMixed(_) => self.as_large_mixed().downcast(small_offsets),
            GeoDataType::GeometryCollection(_) => {
                self.as_geometry_collection().downcast(small_offsets)
            }
            GeoDataType::LargeGeometryCollection(_) => {
                self.as_large_geometry_collection().downcast(small_offsets)
            }
            GeoDataType::Rect => self.as_rect().downcast(small_offsets),
            GeoDataType::WKB => Arc::new(self.as_wkb().clone()),
            GeoDataType::LargeWKB => Arc::new(self.as_large_wkb().clone()),
        }
    }
}

/// Given a set of types, return a single type that the result should be casted to
fn resolve_types(types: &HashSet<GeoDataType>) -> GeoDataType {
    if types.is_empty() {
        panic!("empty types");
    } else if types.len() == 1 {
        *types.iter().next().unwrap()
    } else if types.len() == 2 {
        let mut extension_name_set = HashSet::new();
        // let mut coord_types = HashSet::new();
        types.iter().for_each(|t| {
            extension_name_set.insert(t.extension_name());
        });
        if extension_name_set.contains("geoarrow.point")
            && extension_name_set.contains("geoarrow.multipoint")
        {
            GeoDataType::MultiPoint(Default::default())
        } else if extension_name_set.contains("geoarrow.linestring")
            && extension_name_set.contains("geoarrow.multilinestring")
        {
            GeoDataType::MultiLineString(Default::default())
        } else if extension_name_set.contains("geoarrow.polygon")
            && extension_name_set.contains("geoarrow.multipolygon")
        {
            GeoDataType::MultiPolygon(Default::default())
        } else if extension_name_set.contains("geoarrow.geometrycollection") {
            GeoDataType::GeometryCollection(Default::default())
        } else {
            GeoDataType::Mixed(Default::default())
        }
    } else {
        GeoDataType::Mixed(Default::default())
    }
}

impl Downcast for ChunkedPointArray {
    type Output = Arc<dyn ChunkedGeometryArrayTrait>;

    fn downcasted_data_type(&self, small_offsets: bool) -> GeoDataType {
        *self.data_type()
    }
    fn downcast(&self, small_offsets: bool) -> Self::Output {
        Arc::new(self.clone())
    }
}

macro_rules! impl_chunked_downcast {
    ($chunked_array:ty) => {
        impl<O: OffsetSizeTrait> Downcast for $chunked_array {
            type Output = Arc<dyn ChunkedGeometryArrayTrait>;

            fn downcasted_data_type(&self, small_offsets: bool) -> GeoDataType {
                let mut types = HashSet::new();
                self.chunks.iter().for_each(|chunk| {
                    types.insert(chunk.downcasted_data_type(small_offsets));
                });
                resolve_types(&types)
            }
            fn downcast(&self, small_offsets: bool) -> Self::Output {
                let to_data_type = self.downcasted_data_type(small_offsets);

                if to_data_type == *self.data_type() {
                    return Arc::new(self.clone());
                }

                self.cast(&to_data_type).unwrap()
            }
        }
    };
}

impl_chunked_downcast!(ChunkedLineStringArray<O>);
impl_chunked_downcast!(ChunkedPolygonArray<O>);
impl_chunked_downcast!(ChunkedMultiPointArray<O>);
impl_chunked_downcast!(ChunkedMultiLineStringArray<O>);
impl_chunked_downcast!(ChunkedMultiPolygonArray<O>);
impl_chunked_downcast!(ChunkedMixedGeometryArray<O>);
impl_chunked_downcast!(ChunkedGeometryCollectionArray<O>);

impl Downcast for ChunkedRectArray {
    type Output = Arc<dyn ChunkedGeometryArrayTrait>;

    fn downcasted_data_type(&self, small_offsets: bool) -> GeoDataType {
        *self.data_type()
    }
    fn downcast(&self, small_offsets: bool) -> Self::Output {
        Arc::new(self.clone())
    }
}

impl Downcast for &dyn ChunkedGeometryArrayTrait {
    type Output = Arc<dyn ChunkedGeometryArrayTrait>;

    fn downcasted_data_type(&self, small_offsets: bool) -> GeoDataType {
        match self.data_type() {
            GeoDataType::Point(_) => self.as_point().downcasted_data_type(small_offsets),
            GeoDataType::LineString(_) => self.as_line_string().downcasted_data_type(small_offsets),
            GeoDataType::LargeLineString(_) => self
                .as_large_line_string()
                .downcasted_data_type(small_offsets),
            GeoDataType::Polygon(_) => self.as_polygon().downcasted_data_type(small_offsets),
            GeoDataType::LargePolygon(_) => {
                self.as_large_polygon().downcasted_data_type(small_offsets)
            }
            GeoDataType::MultiPoint(_) => self.as_multi_point().downcasted_data_type(small_offsets),
            GeoDataType::LargeMultiPoint(_) => self
                .as_large_multi_point()
                .downcasted_data_type(small_offsets),
            GeoDataType::MultiLineString(_) => self
                .as_multi_line_string()
                .downcasted_data_type(small_offsets),
            GeoDataType::LargeMultiLineString(_) => self
                .as_large_multi_line_string()
                .downcasted_data_type(small_offsets),
            GeoDataType::MultiPolygon(_) => {
                self.as_multi_polygon().downcasted_data_type(small_offsets)
            }
            GeoDataType::LargeMultiPolygon(_) => self
                .as_large_multi_polygon()
                .downcasted_data_type(small_offsets),
            GeoDataType::Mixed(_) => self.as_mixed().downcasted_data_type(small_offsets),
            GeoDataType::LargeMixed(_) => self.as_large_mixed().downcasted_data_type(small_offsets),
            GeoDataType::GeometryCollection(_) => self
                .as_geometry_collection()
                .downcasted_data_type(small_offsets),
            GeoDataType::LargeGeometryCollection(_) => self
                .as_large_geometry_collection()
                .downcasted_data_type(small_offsets),
            GeoDataType::Rect => self.as_rect().downcasted_data_type(small_offsets),
            _ => todo!(),
        }
    }

    fn downcast(&self, small_offsets: bool) -> Self::Output {
        match self.data_type() {
            GeoDataType::Point(_) => self.as_point().downcast(small_offsets),
            GeoDataType::LineString(_) => self.as_line_string().downcast(small_offsets),
            GeoDataType::LargeLineString(_) => self.as_large_line_string().downcast(small_offsets),
            GeoDataType::Polygon(_) => self.as_polygon().downcast(small_offsets),
            GeoDataType::LargePolygon(_) => self.as_large_polygon().downcast(small_offsets),
            GeoDataType::MultiPoint(_) => self.as_multi_point().downcast(small_offsets),
            GeoDataType::LargeMultiPoint(_) => self.as_large_multi_point().downcast(small_offsets),
            GeoDataType::MultiLineString(_) => self.as_multi_line_string().downcast(small_offsets),
            GeoDataType::LargeMultiLineString(_) => {
                self.as_large_multi_line_string().downcast(small_offsets)
            }
            GeoDataType::MultiPolygon(_) => self.as_multi_polygon().downcast(small_offsets),
            GeoDataType::LargeMultiPolygon(_) => {
                self.as_large_multi_polygon().downcast(small_offsets)
            }
            GeoDataType::Mixed(_) => self.as_mixed().downcast(small_offsets),
            GeoDataType::LargeMixed(_) => self.as_large_mixed().downcast(small_offsets),
            GeoDataType::GeometryCollection(_) => {
                self.as_geometry_collection().downcast(small_offsets)
            }
            GeoDataType::LargeGeometryCollection(_) => {
                self.as_large_geometry_collection().downcast(small_offsets)
            }
            GeoDataType::Rect => self.as_rect().downcast(small_offsets),
            GeoDataType::WKB => Arc::new(self.as_wkb().clone()),
            GeoDataType::LargeWKB => Arc::new(self.as_large_wkb().clone()),
        }
    }
}

impl Downcast for GeoTable {
    type Output = Result<GeoTable>;

    fn downcasted_data_type(&self, small_offsets: bool) -> GeoDataType {
        self.geometry_data_type().unwrap()
    }
    fn downcast(&self, small_offsets: bool) -> Self::Output {
        let downcasted_chunked_geometry = self.geometry()?.as_ref().downcast(small_offsets);

        let (schema, batches, geometry_column_index) = self.clone().into_inner();

        // Keep all fields except the existing geometry field
        let mut new_fields = schema
            .fields()
            .iter()
            .enumerate()
            .filter_map(|(i, field)| {
                if i == geometry_column_index {
                    None
                } else {
                    Some(field.clone())
                }
            })
            .collect::<Vec<_>>();

        // Add the new geometry column at the end of the new fields
        new_fields.push(downcasted_chunked_geometry.extension_field());
        let new_geometry_column_index = new_fields.len() - 1;

        // Construct a new schema with the new fields
        let new_schema = Arc::new(Schema::new(new_fields).with_metadata(schema.metadata.clone()));

        assert_eq!(batches.len(), downcasted_chunked_geometry.num_chunks());
        let new_batches = batches
            .into_iter()
            .zip(downcasted_chunked_geometry.geometry_chunks())
            .map(|(mut batch, geom_chunk)| {
                batch.remove_column(geometry_column_index);
                let mut columns = batch.columns().to_vec();
                columns.push(geom_chunk.to_array_ref());
                RecordBatch::try_new(new_schema.clone(), columns).unwrap()
            })
            .collect();

        GeoTable::try_new(new_schema.clone(), new_batches, new_geometry_column_index)
    }
}

// impl<O: OffsetSizeTrait> Downcast for ChunkedMultiPointArray<O> {
//     type Output = Arc<dyn ChunkedGeometryArrayTrait>;

//     fn downcast(&self) -> Self::Output {
//         let data_types = self.chunks.iter().map(|chunk| chunk.downcasted_data_type()).collect::<Vec<_>>();
//         let data_types_same = data_types.windows(2).all(|w| w[0] == w[1]);
//         if !data_types_same {
//             return Arc::new(self.clone());
//         }

//         //  else {
//         //     let x = ChunkedGeometryArray::new(self.chunks.iter().map(|chunk| chunk.downcast()).collect());

//         // }

//     }
// }
