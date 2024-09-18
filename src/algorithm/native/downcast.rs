#![allow(unused_variables)]

use std::collections::HashSet;
use std::sync::Arc;

use arrow_array::OffsetSizeTrait;
use arrow_buffer::OffsetBuffer;

use crate::algorithm::native::cast::Cast;
use crate::array::offset_builder::OffsetsBuilder;
use crate::array::util::OffsetBufferUtils;
use crate::array::*;
use crate::chunked_array::*;
use crate::datatypes::{Dimension, GeoDataType};
use crate::error::Result;
use crate::schema::GeoSchemaExt;
use crate::table::Table;
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

impl<const D: usize> Downcast for PointArray<D> {
    type Output = Arc<dyn GeometryArrayTrait>;

    fn downcasted_data_type(&self, small_offsets: bool) -> GeoDataType {
        self.data_type()
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
#[allow(dead_code)]
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
pub(crate) fn can_downcast_multi<O: OffsetSizeTrait>(buffer: &OffsetBuffer<O>) -> bool {
    buffer
        .windows(2)
        .all(|slice| *slice.get(1).unwrap() - *slice.first().unwrap() <= O::one())
}

impl<O: OffsetSizeTrait, const D: usize> Downcast for LineStringArray<O, D> {
    type Output = Arc<dyn GeometryArrayTrait>;

    fn downcasted_data_type(&self, small_offsets: bool) -> GeoDataType {
        use GeoDataType::*;
        match self.data_type() {
            LineString(ct, dim) => LineString(ct, dim),
            LargeLineString(ct, dim) => {
                if small_offsets && can_downcast_offsets_i32(&self.geom_offsets) {
                    LineString(ct, dim)
                } else {
                    LargeLineString(ct, dim)
                }
            }
            _ => unreachable!(),
        }
    }

    fn downcast(&self, small_offsets: bool) -> Self::Output {
        use GeoDataType::*;
        match (self.data_type(), self.downcasted_data_type(small_offsets)) {
            (LineString(_, Dimension::XY), LineString(_, Dimension::XY))
            | (LargeLineString(_, Dimension::XY), LargeLineString(_, Dimension::XY)) => {
                Arc::new(self.clone())
            }
            (LargeLineString(_, Dimension::XY), LineString(_, Dimension::XY)) => todo!(),
            _ => unreachable!(),
        }
    }
}

impl<O: OffsetSizeTrait, const D: usize> Downcast for PolygonArray<O, D> {
    type Output = Arc<dyn GeometryArrayTrait>;

    fn downcasted_data_type(&self, small_offsets: bool) -> GeoDataType {
        use GeoDataType::*;
        match self.data_type() {
            Polygon(ct, dim) => Polygon(ct, dim),
            LargePolygon(ct, dim) => {
                if small_offsets && can_downcast_offsets_i32(&self.ring_offsets) {
                    Polygon(ct, dim)
                } else {
                    LargePolygon(ct, dim)
                }
            }
            _ => unreachable!(),
        }
    }

    fn downcast(&self, small_offsets: bool) -> Self::Output {
        Arc::new(self.clone())
    }
}

impl<O: OffsetSizeTrait> Downcast for MultiPointArray<O, 2> {
    type Output = Arc<dyn GeometryArrayTrait>;

    fn downcasted_data_type(&self, small_offsets: bool) -> GeoDataType {
        use GeoDataType::*;
        match self.data_type() {
            MultiPoint(ct, dim) => {
                if can_downcast_multi(&self.geom_offsets) {
                    Point(ct, dim)
                } else {
                    MultiPoint(ct, dim)
                }
            }
            LargeMultiPoint(ct, dim) => {
                match (
                    can_downcast_multi(&self.geom_offsets),
                    small_offsets && can_downcast_offsets_i32(&self.geom_offsets),
                ) {
                    (true, _) => Point(ct, dim),
                    (false, true) => MultiPoint(ct, dim),
                    (false, false) => LargeMultiPoint(ct, dim),
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

impl<O: OffsetSizeTrait> Downcast for MultiLineStringArray<O, 2> {
    type Output = Arc<dyn GeometryArrayTrait>;

    fn downcasted_data_type(&self, small_offsets: bool) -> GeoDataType {
        match self.data_type() {
            GeoDataType::MultiLineString(ct, dim) => {
                if can_downcast_multi(&self.geom_offsets) {
                    GeoDataType::LineString(ct, dim)
                } else {
                    GeoDataType::MultiLineString(ct, dim)
                }
            }
            GeoDataType::LargeMultiLineString(ct, dim) => {
                match (
                    can_downcast_multi(&self.geom_offsets),
                    small_offsets && can_downcast_offsets_i32(&self.ring_offsets),
                ) {
                    (true, true) => GeoDataType::LineString(ct, dim),
                    (true, false) => GeoDataType::LargeLineString(ct, dim),
                    (false, true) => GeoDataType::MultiLineString(ct, dim),
                    (false, false) => GeoDataType::LargeMultiLineString(ct, dim),
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

impl<O: OffsetSizeTrait> Downcast for MultiPolygonArray<O, 2> {
    type Output = Arc<dyn GeometryArrayTrait>;

    fn downcasted_data_type(&self, small_offsets: bool) -> GeoDataType {
        match self.data_type() {
            GeoDataType::MultiPolygon(ct, dim) => {
                if can_downcast_multi(&self.geom_offsets) {
                    GeoDataType::Polygon(ct, dim)
                } else {
                    GeoDataType::MultiPolygon(ct, dim)
                }
            }
            GeoDataType::LargeMultiPolygon(ct, dim) => {
                match (
                    can_downcast_multi(&self.geom_offsets),
                    small_offsets && can_downcast_offsets_i32(&self.ring_offsets),
                ) {
                    (true, true) => GeoDataType::Polygon(ct, dim),
                    (true, false) => GeoDataType::LargePolygon(ct, dim),
                    (false, true) => GeoDataType::MultiPolygon(ct, dim),
                    (false, false) => GeoDataType::LargeMultiPolygon(ct, dim),
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

impl<O: OffsetSizeTrait> Downcast for MixedGeometryArray<O, 2> {
    type Output = Arc<dyn GeometryArrayTrait>;

    fn downcasted_data_type(&self, small_offsets: bool) -> GeoDataType {
        let coord_type = self.coord_type();

        if self.has_points()
            && !self.has_line_strings()
            && !self.has_polygons()
            && !self.has_multi_points()
            && !self.has_multi_line_strings()
            && !self.has_multi_polygons()
        {
            return GeoDataType::Point(coord_type, Dimension::XY);
        }

        if !self.has_points()
            && self.has_line_strings()
            && !self.has_polygons()
            && !self.has_multi_points()
            && !self.has_multi_line_strings()
            && !self.has_multi_polygons()
        {
            return self.line_strings.downcasted_data_type(small_offsets);
        }

        if !self.has_points()
            && !self.has_line_strings()
            && self.has_polygons()
            && !self.has_multi_points()
            && !self.has_multi_line_strings()
            && !self.has_multi_polygons()
        {
            return self.polygons.downcasted_data_type(small_offsets);
        }

        if !self.has_points()
            && !self.has_line_strings()
            && !self.has_polygons()
            && self.has_multi_points()
            && !self.has_multi_line_strings()
            && !self.has_multi_polygons()
        {
            return self.multi_points.downcasted_data_type(small_offsets);
        }

        if !self.has_points()
            && !self.has_line_strings()
            && !self.has_polygons()
            && !self.has_multi_points()
            && self.has_multi_line_strings()
            && !self.has_multi_polygons()
        {
            return self.multi_line_strings.downcasted_data_type(small_offsets);
        }

        if !self.has_points()
            && !self.has_line_strings()
            && !self.has_polygons()
            && !self.has_multi_points()
            && !self.has_multi_line_strings()
            && self.has_multi_polygons()
        {
            return self.multi_polygons.downcasted_data_type(small_offsets);
        }

        self.data_type()
    }

    fn downcast(&self, small_offsets: bool) -> Self::Output {
        // TODO: do I need to handle the slice offset?
        if self.has_points()
            && !self.has_line_strings()
            && !self.has_polygons()
            && !self.has_multi_points()
            && !self.has_multi_line_strings()
            && !self.has_multi_polygons()
        {
            return Arc::new(self.points.clone());
        }

        if !self.has_points()
            && self.has_line_strings()
            && !self.has_polygons()
            && !self.has_multi_points()
            && !self.has_multi_line_strings()
            && !self.has_multi_polygons()
        {
            return self.line_strings.downcast(small_offsets);
        }

        if !self.has_points()
            && !self.has_line_strings()
            && self.has_polygons()
            && !self.has_multi_points()
            && !self.has_multi_line_strings()
            && !self.has_multi_polygons()
        {
            return self.polygons.downcast(small_offsets);
        }

        if !self.has_points()
            && !self.has_line_strings()
            && !self.has_polygons()
            && self.has_multi_points()
            && !self.has_multi_line_strings()
            && !self.has_multi_polygons()
        {
            return self.multi_points.downcast(small_offsets);
        }

        if !self.has_points()
            && !self.has_line_strings()
            && !self.has_polygons()
            && !self.has_multi_points()
            && self.has_multi_line_strings()
            && !self.has_multi_polygons()
        {
            return self.multi_line_strings.downcast(small_offsets);
        }

        if !self.has_points()
            && !self.has_line_strings()
            && !self.has_polygons()
            && !self.has_multi_points()
            && !self.has_multi_line_strings()
            && self.has_multi_polygons()
        {
            return self.multi_polygons.downcast(small_offsets);
        }

        Arc::new(self.clone())
    }
}

impl<O: OffsetSizeTrait> Downcast for GeometryCollectionArray<O, 2> {
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

impl Downcast for RectArray<2> {
    type Output = Arc<dyn GeometryArrayTrait>;

    fn downcasted_data_type(&self, small_offsets: bool) -> GeoDataType {
        self.data_type()
    }
    fn downcast(&self, small_offsets: bool) -> Self::Output {
        Arc::new(self.clone())
    }
}

impl Downcast for &dyn GeometryArrayTrait {
    type Output = Arc<dyn GeometryArrayTrait>;

    fn downcasted_data_type(&self, small_offsets: bool) -> GeoDataType {
        use Dimension::*;
        use GeoDataType::*;

        match self.data_type() {
            Point(_, XY) => self.as_point::<2>().downcasted_data_type(small_offsets),
            LineString(_, XY) => self
                .as_line_string::<2>()
                .downcasted_data_type(small_offsets),
            LargeLineString(_, XY) => self
                .as_large_line_string::<2>()
                .downcasted_data_type(small_offsets),
            Polygon(_, XY) => self.as_polygon::<2>().downcasted_data_type(small_offsets),
            LargePolygon(_, XY) => self
                .as_large_polygon::<2>()
                .downcasted_data_type(small_offsets),
            MultiPoint(_, XY) => self
                .as_multi_point::<2>()
                .downcasted_data_type(small_offsets),
            LargeMultiPoint(_, XY) => self
                .as_large_multi_point::<2>()
                .downcasted_data_type(small_offsets),
            MultiLineString(_, XY) => self
                .as_multi_line_string::<2>()
                .downcasted_data_type(small_offsets),
            LargeMultiLineString(_, XY) => self
                .as_large_multi_line_string::<2>()
                .downcasted_data_type(small_offsets),
            MultiPolygon(_, XY) => self
                .as_multi_polygon::<2>()
                .downcasted_data_type(small_offsets),
            LargeMultiPolygon(_, XY) => self
                .as_large_multi_polygon::<2>()
                .downcasted_data_type(small_offsets),
            Mixed(_, XY) => self.as_mixed::<2>().downcasted_data_type(small_offsets),
            LargeMixed(_, XY) => self
                .as_large_mixed::<2>()
                .downcasted_data_type(small_offsets),
            GeometryCollection(_, XY) => self
                .as_geometry_collection::<2>()
                .downcasted_data_type(small_offsets),
            LargeGeometryCollection(_, XY) => self
                .as_large_geometry_collection::<2>()
                .downcasted_data_type(small_offsets),
            Rect(XY) => self.as_rect::<2>().downcasted_data_type(small_offsets),
            // TODO: downcast largewkb to wkb
            WKB => self.data_type(),
            LargeWKB => self.data_type(),
            _ => todo!("3d support"),
        }
    }

    fn downcast(&self, small_offsets: bool) -> Self::Output {
        use Dimension::*;
        use GeoDataType::*;

        match self.data_type() {
            Point(_, XY) => self.as_point::<2>().downcast(small_offsets),
            LineString(_, XY) => self.as_line_string::<2>().downcast(small_offsets),
            LargeLineString(_, XY) => self.as_large_line_string::<2>().downcast(small_offsets),
            Polygon(_, XY) => self.as_polygon::<2>().downcast(small_offsets),
            LargePolygon(_, XY) => self.as_large_polygon::<2>().downcast(small_offsets),
            MultiPoint(_, XY) => self.as_multi_point::<2>().downcast(small_offsets),
            LargeMultiPoint(_, XY) => self.as_large_multi_point::<2>().downcast(small_offsets),
            MultiLineString(_, XY) => self.as_multi_line_string::<2>().downcast(small_offsets),
            LargeMultiLineString(_, XY) => self
                .as_large_multi_line_string::<2>()
                .downcast(small_offsets),
            MultiPolygon(_, XY) => self.as_multi_polygon::<2>().downcast(small_offsets),
            LargeMultiPolygon(_, XY) => self.as_large_multi_polygon::<2>().downcast(small_offsets),
            Mixed(_, XY) => self.as_mixed::<2>().downcast(small_offsets),
            LargeMixed(_, XY) => self.as_large_mixed::<2>().downcast(small_offsets),
            GeometryCollection(_, XY) => self.as_geometry_collection::<2>().downcast(small_offsets),
            LargeGeometryCollection(_, XY) => self
                .as_large_geometry_collection::<2>()
                .downcast(small_offsets),
            Rect(XY) => self.as_rect::<2>().downcast(small_offsets),
            WKB => Arc::new(self.as_wkb().clone()),
            LargeWKB => Arc::new(self.as_large_wkb().clone()),
            _ => todo!("3d support"),
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
            GeoDataType::MultiPoint(Default::default(), Dimension::XY)
        } else if extension_name_set.contains("geoarrow.linestring")
            && extension_name_set.contains("geoarrow.multilinestring")
        {
            GeoDataType::MultiLineString(Default::default(), Dimension::XY)
        } else if extension_name_set.contains("geoarrow.polygon")
            && extension_name_set.contains("geoarrow.multipolygon")
        {
            GeoDataType::MultiPolygon(Default::default(), Dimension::XY)
        } else if extension_name_set.contains("geoarrow.geometrycollection") {
            GeoDataType::GeometryCollection(Default::default(), Dimension::XY)
        } else {
            GeoDataType::Mixed(Default::default(), Dimension::XY)
        }
    } else {
        GeoDataType::Mixed(Default::default(), Dimension::XY)
    }
}

impl Downcast for ChunkedPointArray<2> {
    type Output = Arc<dyn ChunkedGeometryArrayTrait>;

    fn downcasted_data_type(&self, small_offsets: bool) -> GeoDataType {
        self.data_type()
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

                if to_data_type == self.data_type() {
                    return Arc::new(self.clone());
                }

                self.cast(&to_data_type).unwrap()
            }
        }
    };
}

impl_chunked_downcast!(ChunkedLineStringArray<O, 2>);
impl_chunked_downcast!(ChunkedPolygonArray<O, 2>);
impl_chunked_downcast!(ChunkedMultiPointArray<O, 2>);
impl_chunked_downcast!(ChunkedMultiLineStringArray<O, 2>);
impl_chunked_downcast!(ChunkedMultiPolygonArray<O, 2>);
impl_chunked_downcast!(ChunkedMixedGeometryArray<O, 2>);
impl_chunked_downcast!(ChunkedGeometryCollectionArray<O, 2>);

impl Downcast for ChunkedRectArray<2> {
    type Output = Arc<dyn ChunkedGeometryArrayTrait>;

    fn downcasted_data_type(&self, small_offsets: bool) -> GeoDataType {
        self.data_type()
    }
    fn downcast(&self, small_offsets: bool) -> Self::Output {
        Arc::new(self.clone())
    }
}

impl Downcast for &dyn ChunkedGeometryArrayTrait {
    type Output = Arc<dyn ChunkedGeometryArrayTrait>;

    fn downcasted_data_type(&self, small_offsets: bool) -> GeoDataType {
        use Dimension::*;
        use GeoDataType::*;

        match self.data_type() {
            Point(_, XY) => self.as_point::<2>().downcasted_data_type(small_offsets),
            LineString(_, XY) => self
                .as_line_string::<2>()
                .downcasted_data_type(small_offsets),
            LargeLineString(_, XY) => self
                .as_large_line_string::<2>()
                .downcasted_data_type(small_offsets),
            Polygon(_, XY) => self.as_polygon::<2>().downcasted_data_type(small_offsets),
            LargePolygon(_, XY) => self
                .as_large_polygon::<2>()
                .downcasted_data_type(small_offsets),
            MultiPoint(_, XY) => self
                .as_multi_point::<2>()
                .downcasted_data_type(small_offsets),
            LargeMultiPoint(_, XY) => self
                .as_large_multi_point::<2>()
                .downcasted_data_type(small_offsets),
            MultiLineString(_, XY) => self
                .as_multi_line_string::<2>()
                .downcasted_data_type(small_offsets),
            LargeMultiLineString(_, XY) => self
                .as_large_multi_line_string::<2>()
                .downcasted_data_type(small_offsets),
            MultiPolygon(_, XY) => self
                .as_multi_polygon::<2>()
                .downcasted_data_type(small_offsets),
            LargeMultiPolygon(_, XY) => self
                .as_large_multi_polygon::<2>()
                .downcasted_data_type(small_offsets),
            Mixed(_, XY) => self.as_mixed::<2>().downcasted_data_type(small_offsets),
            LargeMixed(_, XY) => self
                .as_large_mixed::<2>()
                .downcasted_data_type(small_offsets),
            GeometryCollection(_, XY) => self
                .as_geometry_collection::<2>()
                .downcasted_data_type(small_offsets),
            LargeGeometryCollection(_, XY) => self
                .as_large_geometry_collection::<2>()
                .downcasted_data_type(small_offsets),
            Rect(XY) => self.as_rect::<2>().downcasted_data_type(small_offsets),
            _ => todo!("3d support"),
        }
    }

    fn downcast(&self, small_offsets: bool) -> Self::Output {
        use Dimension::*;
        use GeoDataType::*;

        match self.data_type() {
            Point(_, XY) => self.as_point::<2>().downcast(small_offsets),
            LineString(_, XY) => self.as_line_string::<2>().downcast(small_offsets),
            LargeLineString(_, XY) => self.as_large_line_string::<2>().downcast(small_offsets),
            Polygon(_, XY) => self.as_polygon::<2>().downcast(small_offsets),
            LargePolygon(_, XY) => self.as_large_polygon::<2>().downcast(small_offsets),
            MultiPoint(_, XY) => self.as_multi_point::<2>().downcast(small_offsets),
            LargeMultiPoint(_, XY) => self.as_large_multi_point::<2>().downcast(small_offsets),
            MultiLineString(_, XY) => self.as_multi_line_string::<2>().downcast(small_offsets),
            LargeMultiLineString(_, XY) => self
                .as_large_multi_line_string::<2>()
                .downcast(small_offsets),
            MultiPolygon(_, XY) => self.as_multi_polygon::<2>().downcast(small_offsets),
            LargeMultiPolygon(_, XY) => self.as_large_multi_polygon::<2>().downcast(small_offsets),
            Mixed(_, XY) => self.as_mixed::<2>().downcast(small_offsets),
            LargeMixed(_, XY) => self.as_large_mixed::<2>().downcast(small_offsets),
            GeometryCollection(_, XY) => self.as_geometry_collection::<2>().downcast(small_offsets),
            LargeGeometryCollection(_, XY) => self
                .as_large_geometry_collection::<2>()
                .downcast(small_offsets),
            Rect(XY) => self.as_rect::<2>().downcast(small_offsets),
            WKB => Arc::new(self.as_wkb().clone()),
            LargeWKB => Arc::new(self.as_large_wkb().clone()),
            _ => todo!("3d support"),
        }
    }
}

pub trait DowncastTable {
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
    fn downcast(&self, small_offsets: bool) -> Result<Table>;
}

impl DowncastTable for Table {
    fn downcast(&self, small_offsets: bool) -> Result<Table> {
        let downcasted_columns = self
            .schema()
            .as_ref()
            .geometry_columns()
            .iter()
            .map(|idx| {
                let geometry = self.geometry_column(Some(*idx))?;
                Ok((*idx, geometry.as_ref().downcast(small_offsets)))
            })
            .collect::<Result<Vec<_>>>()?;

        let mut new_table = self.clone();

        for (column_idx, column) in downcasted_columns.iter() {
            let prev_field = self.schema().field(*column_idx);
            let new_field = column
                .data_type()
                .to_field(prev_field.name(), prev_field.is_nullable());
            new_table.set_column(*column_idx, new_field.into(), column.array_refs())?;
        }

        Ok(new_table)
    }
}

// impl<O: OffsetSizeTrait> Downcast for ChunkedMultiPointArray<O, 2> {
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
