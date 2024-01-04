#![allow(unused_variables, dead_code)]

use std::collections::HashSet;
use std::sync::Arc;

use arrow_array::{OffsetSizeTrait, RecordBatch};
use arrow_buffer::OffsetBuffer;
use arrow_schema::Schema;

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

/// If the offset type `O` is already `i32`, will return false
fn can_downcast_offsets<O: OffsetSizeTrait>(buffer: &OffsetBuffer<O>) -> bool {
    if O::IS_LARGE {
        buffer.last().to_usize().unwrap() < i32::MAX as usize
    } else {
        false
    }
}

/// Downcast an i64 offset buffer to i32
fn downcast_offsets<O: OffsetSizeTrait>(buffer: &OffsetBuffer<O>) -> OffsetBuffer<i32> {
    if O::IS_LARGE {
        let mut builder = OffsetsBuilder::with_capacity(buffer.len_proxy());
        buffer
            .iter()
            .for_each(|x| builder.try_push(x.to_usize().unwrap() as i32).unwrap());
        builder.finish()
    } else {
        todo!()
    }
}

impl<O: OffsetSizeTrait> Downcast for LineStringArray<O> {
    type Output = Arc<dyn GeometryArrayTrait>;

    fn downcasted_data_type(&self, small_offsets: bool) -> GeoDataType {
        match self.data_type() {
            GeoDataType::LineString(ct) => GeoDataType::LineString(*ct),
            GeoDataType::LargeLineString(ct) => {
                if small_offsets && can_downcast_offsets(&self.geom_offsets) {
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
        todo!()
    }
    fn downcast(&self, small_offsets: bool) -> Self::Output {
        Arc::new(self.clone())
    }
}

impl<O: OffsetSizeTrait> Downcast for MultiPointArray<O> {
    type Output = Arc<dyn GeometryArrayTrait>;

    fn downcasted_data_type(&self, small_offsets: bool) -> GeoDataType {
        todo!()
    }
    fn downcast(&self, small_offsets: bool) -> Self::Output {
        // Note: this won't allow a downcast for empty MultiPoints
        if self.geom_offsets.last().to_usize().unwrap() == self.len() {
            return Arc::new(PointArray::new(self.coords.clone(), self.validity.clone()));
        }

        Arc::new(self.clone())
    }
}

impl<O: OffsetSizeTrait> Downcast for MultiLineStringArray<O> {
    type Output = Arc<dyn GeometryArrayTrait>;

    fn downcasted_data_type(&self, small_offsets: bool) -> GeoDataType {
        todo!()
    }
    fn downcast(&self, small_offsets: bool) -> Self::Output {
        if self.geom_offsets.last().to_usize().unwrap() == self.len() {
            return Arc::new(LineStringArray::new(
                self.coords.clone(),
                self.ring_offsets.clone(),
                self.validity.clone(),
            ));
        }

        Arc::new(self.clone())
    }
}

impl<O: OffsetSizeTrait> Downcast for MultiPolygonArray<O> {
    type Output = Arc<dyn GeometryArrayTrait>;

    fn downcasted_data_type(&self, small_offsets: bool) -> GeoDataType {
        todo!()
    }
    fn downcast(&self, small_offsets: bool) -> Self::Output {
        if self.geom_offsets.last().to_usize().unwrap() == self.len() {
            return Arc::new(PolygonArray::new(
                self.coords.clone(),
                self.polygon_offsets.clone(),
                self.ring_offsets.clone(),
                self.validity.clone(),
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
            return if O::IS_LARGE {
                GeoDataType::LargeLineString(coord_type)
            } else {
                GeoDataType::LineString(coord_type)
            };
        }

        if self.points.is_none()
            && self.line_strings.is_none()
            && self.polygons.is_some()
            && self.multi_points.is_none()
            && self.multi_line_strings.is_none()
            && self.multi_polygons.is_none()
        {
            return if O::IS_LARGE {
                GeoDataType::LargePolygon(coord_type)
            } else {
                GeoDataType::Polygon(coord_type)
            };
        }

        if self.points.is_none()
            && self.line_strings.is_none()
            && self.polygons.is_none()
            && self.multi_points.is_some()
            && self.multi_line_strings.is_none()
            && self.multi_polygons.is_none()
        {
            return if O::IS_LARGE {
                GeoDataType::LargeMultiPoint(coord_type)
            } else {
                GeoDataType::MultiPoint(coord_type)
            };
        }

        if self.points.is_none()
            && self.line_strings.is_none()
            && self.polygons.is_none()
            && self.multi_points.is_none()
            && self.multi_line_strings.is_some()
            && self.multi_polygons.is_none()
        {
            return if O::IS_LARGE {
                GeoDataType::LargeMultiLineString(coord_type)
            } else {
                GeoDataType::MultiLineString(coord_type)
            };
        }

        if self.points.is_none()
            && self.line_strings.is_none()
            && self.polygons.is_none()
            && self.multi_points.is_none()
            && self.multi_line_strings.is_none()
            && self.multi_polygons.is_some()
        {
            return if O::IS_LARGE {
                GeoDataType::LargeMultiPolygon(coord_type)
            } else {
                GeoDataType::MultiPolygon(coord_type)
            };
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
            return Arc::new(self.line_strings.as_ref().unwrap().clone());
        }

        if self.points.is_none()
            && self.line_strings.is_none()
            && self.polygons.is_some()
            && self.multi_points.is_none()
            && self.multi_line_strings.is_none()
            && self.multi_polygons.is_none()
        {
            return Arc::new(self.polygons.as_ref().unwrap().clone());
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
        todo!()
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

impl Downcast for ChunkedPointArray {
    type Output = Arc<dyn ChunkedGeometryArrayTrait>;

    fn downcasted_data_type(&self, small_offsets: bool) -> GeoDataType {
        *self.data_type()
    }
    fn downcast(&self, small_offsets: bool) -> Self::Output {
        Arc::new(self.clone())
    }
}

impl<O: OffsetSizeTrait> Downcast for ChunkedLineStringArray<O> {
    type Output = Arc<dyn ChunkedGeometryArrayTrait>;

    fn downcasted_data_type(&self, small_offsets: bool) -> GeoDataType {
        *self.data_type()
    }
    fn downcast(&self, small_offsets: bool) -> Self::Output {
        Arc::new(self.clone())
    }
}

impl<O: OffsetSizeTrait> Downcast for ChunkedPolygonArray<O> {
    type Output = Arc<dyn ChunkedGeometryArrayTrait>;

    fn downcasted_data_type(&self, small_offsets: bool) -> GeoDataType {
        *self.data_type()
    }
    fn downcast(&self, small_offsets: bool) -> Self::Output {
        Arc::new(self.clone())
    }
}

impl<O: OffsetSizeTrait> Downcast for ChunkedMultiPointArray<O> {
    type Output = Arc<dyn ChunkedGeometryArrayTrait>;

    fn downcasted_data_type(&self, small_offsets: bool) -> GeoDataType {
        *self.data_type()
    }
    fn downcast(&self, small_offsets: bool) -> Self::Output {
        Arc::new(self.clone())
    }
}

impl<O: OffsetSizeTrait> Downcast for ChunkedMultiLineStringArray<O> {
    type Output = Arc<dyn ChunkedGeometryArrayTrait>;

    fn downcasted_data_type(&self, small_offsets: bool) -> GeoDataType {
        *self.data_type()
    }
    fn downcast(&self, small_offsets: bool) -> Self::Output {
        Arc::new(self.clone())
    }
}

impl<O: OffsetSizeTrait> Downcast for ChunkedMultiPolygonArray<O> {
    type Output = Arc<dyn ChunkedGeometryArrayTrait>;

    fn downcasted_data_type(&self, small_offsets: bool) -> GeoDataType {
        *self.data_type()
    }
    fn downcast(&self, small_offsets: bool) -> Self::Output {
        Arc::new(self.clone())
    }
}

impl<O: OffsetSizeTrait> Downcast for ChunkedMixedGeometryArray<O> {
    type Output = Arc<dyn ChunkedGeometryArrayTrait>;

    fn downcasted_data_type(&self, small_offsets: bool) -> GeoDataType {
        let mut data_types = HashSet::new();
        self.chunks.iter().for_each(|chunk| {
            data_types.insert(chunk.downcasted_data_type(small_offsets));
        });
        if data_types.len() == 1 {
            // A single data type we can downcast to
            data_types.drain().next().unwrap()
        } else {
            *self.data_type()
        }
    }
    fn downcast(&self, small_offsets: bool) -> Self::Output {
        // Nothing to downcast
        if self.downcasted_data_type(small_offsets) == *self.data_type() {
            return Arc::new(self.clone());
        }

        let downcasted_chunks = self
            .chunks
            .iter()
            .map(|chunk| chunk.downcast(small_offsets))
            .collect::<Vec<_>>();
        match self.downcasted_data_type(small_offsets) {
            GeoDataType::Point(_) => Arc::new(ChunkedGeometryArray::new(
                downcasted_chunks
                    .into_iter()
                    .map(|chunk| chunk.as_ref().as_point().clone())
                    .collect(),
            )),
            GeoDataType::LineString(_) => Arc::new(ChunkedGeometryArray::new(
                downcasted_chunks
                    .into_iter()
                    .map(|chunk| chunk.as_ref().as_line_string().clone())
                    .collect(),
            )),
            GeoDataType::LargeLineString(_) => Arc::new(ChunkedGeometryArray::new(
                downcasted_chunks
                    .into_iter()
                    .map(|chunk| chunk.as_ref().as_large_line_string().clone())
                    .collect(),
            )),
            GeoDataType::Polygon(_) => Arc::new(ChunkedGeometryArray::new(
                downcasted_chunks
                    .into_iter()
                    .map(|chunk| chunk.as_ref().as_polygon().clone())
                    .collect(),
            )),
            GeoDataType::LargePolygon(_) => Arc::new(ChunkedGeometryArray::new(
                downcasted_chunks
                    .into_iter()
                    .map(|chunk| chunk.as_ref().as_large_polygon().clone())
                    .collect(),
            )),
            GeoDataType::MultiPoint(_) => Arc::new(ChunkedGeometryArray::new(
                downcasted_chunks
                    .into_iter()
                    .map(|chunk| chunk.as_ref().as_multi_point().clone())
                    .collect(),
            )),
            GeoDataType::LargeMultiPoint(_) => Arc::new(ChunkedGeometryArray::new(
                downcasted_chunks
                    .into_iter()
                    .map(|chunk| chunk.as_ref().as_large_multi_point().clone())
                    .collect(),
            )),
            GeoDataType::MultiLineString(_) => Arc::new(ChunkedGeometryArray::new(
                downcasted_chunks
                    .into_iter()
                    .map(|chunk| chunk.as_ref().as_multi_line_string().clone())
                    .collect(),
            )),
            GeoDataType::LargeMultiLineString(_) => Arc::new(ChunkedGeometryArray::new(
                downcasted_chunks
                    .into_iter()
                    .map(|chunk| chunk.as_ref().as_large_multi_line_string().clone())
                    .collect(),
            )),
            GeoDataType::MultiPolygon(_) => Arc::new(ChunkedGeometryArray::new(
                downcasted_chunks
                    .into_iter()
                    .map(|chunk| chunk.as_ref().as_multi_polygon().clone())
                    .collect(),
            )),
            GeoDataType::LargeMultiPolygon(_) => Arc::new(ChunkedGeometryArray::new(
                downcasted_chunks
                    .into_iter()
                    .map(|chunk| chunk.as_ref().as_large_multi_polygon().clone())
                    .collect(),
            )),
            GeoDataType::Mixed(_) => Arc::new(ChunkedGeometryArray::new(
                downcasted_chunks
                    .into_iter()
                    .map(|chunk| chunk.as_ref().as_mixed().clone())
                    .collect(),
            )),
            GeoDataType::LargeMixed(_) => Arc::new(ChunkedGeometryArray::new(
                downcasted_chunks
                    .into_iter()
                    .map(|chunk| chunk.as_ref().as_large_mixed().clone())
                    .collect(),
            )),
            _ => unreachable!(),
        }
    }
}

impl<O: OffsetSizeTrait> Downcast for ChunkedGeometryCollectionArray<O> {
    type Output = Arc<dyn ChunkedGeometryArrayTrait>;

    fn downcasted_data_type(&self, small_offsets: bool) -> GeoDataType {
        *self.data_type()
    }
    fn downcast(&self, small_offsets: bool) -> Self::Output {
        Arc::new(self.clone())
    }
}

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
        todo!()
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
                columns.push(geom_chunk.clone().as_ref().to_array_ref());
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
