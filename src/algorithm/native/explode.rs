use std::sync::Arc;

use arrow::compute::take;
use arrow_array::{Int32Array, OffsetSizeTrait, RecordBatch};
use arrow_buffer::OffsetBuffer;
use arrow_schema::SchemaBuilder;

use crate::array::*;
use crate::chunked_array::{
    from_geoarrow_chunks, ChunkedArray, ChunkedGeometryArray, ChunkedGeometryArrayTrait,
};
use crate::datatypes::{Dimension, GeoDataType};
use crate::error::{GeoArrowError, Result};
use crate::table::Table;
use crate::GeometryArrayTrait;

pub trait Explode {
    type Output;

    /// Returns the exploded geometries and, if an explode needs to happen, the indices that should
    /// be passed into a [`take`][arrow::compute::take] operation.
    fn explode(&self) -> Self::Output;
}

impl Explode for PointArray<2> {
    type Output = (Self, Option<Int32Array>);

    fn explode(&self) -> Self::Output {
        (self.clone(), None)
    }
}

impl<O: OffsetSizeTrait> Explode for LineStringArray<O, 2> {
    type Output = (Self, Option<Int32Array>);

    fn explode(&self) -> Self::Output {
        (self.clone(), None)
    }
}

impl<O: OffsetSizeTrait> Explode for PolygonArray<O, 2> {
    type Output = (Self, Option<Int32Array>);

    fn explode(&self) -> Self::Output {
        (self.clone(), None)
    }
}

/// Convert from offsets into a buffer to indices that need to be taken
///
/// e.g. if `offsets` is `[0, 2, 5, 10]`, then there are 2, 3, and 5 elements. The indices needed
/// for a take to explode this array are
/// ```notest
/// [0, 0, 1, 1, 1, 2, 2, 2, 2, 2]
/// ```
/// Also note that the length of the `indices` created is the same as the last value of the
/// offsets.
fn explode_offsets<O: OffsetSizeTrait>(offsets: &OffsetBuffer<O>) -> Int32Array {
    let mut take_indices: Vec<i32> =
        Vec::with_capacity(offsets.last().unwrap().to_usize().unwrap());
    for (offset_idx, offset_start_end) in offsets.as_ref().windows(2).enumerate() {
        let offset_start = offset_start_end[0].to_usize().unwrap();
        let offset_end = offset_start_end[1].to_usize().unwrap();
        for _ in offset_start..offset_end {
            take_indices.push(offset_idx.try_into().unwrap());
        }
    }
    Int32Array::new(take_indices.into(), None)
}

impl<O: OffsetSizeTrait> Explode for MultiPointArray<O, 2> {
    type Output = (PointArray<2>, Option<Int32Array>);

    fn explode(&self) -> Self::Output {
        assert_eq!(
            self.null_count(),
            0,
            "Null values not yet supported in explode"
        );

        let exploded_geoms = PointArray::new(self.coords.clone(), None, self.metadata());
        let take_indices = explode_offsets(self.geom_offsets());
        (exploded_geoms, Some(take_indices))
    }
}

impl<O: OffsetSizeTrait> Explode for MultiLineStringArray<O, 2> {
    type Output = (LineStringArray<O, 2>, Option<Int32Array>);

    fn explode(&self) -> Self::Output {
        assert_eq!(
            self.null_count(),
            0,
            "Null values not yet supported in explode"
        );

        let exploded_geoms = LineStringArray::new(
            self.coords.clone(),
            self.ring_offsets.clone(),
            None,
            self.metadata(),
        );
        let take_indices = explode_offsets(self.geom_offsets());
        (exploded_geoms, Some(take_indices))
    }
}

impl<O: OffsetSizeTrait> Explode for MultiPolygonArray<O, 2> {
    type Output = (PolygonArray<O, 2>, Option<Int32Array>);

    fn explode(&self) -> Self::Output {
        assert_eq!(
            self.null_count(),
            0,
            "Null values not yet supported in explode"
        );

        let exploded_geoms = PolygonArray::new(
            self.coords.clone(),
            self.polygon_offsets.clone(),
            self.ring_offsets.clone(),
            None,
            self.metadata(),
        );
        let take_indices = explode_offsets(self.geom_offsets());
        (exploded_geoms, Some(take_indices))
    }
}

impl Explode for &dyn GeometryArrayTrait {
    type Output = Result<(Arc<dyn GeometryArrayTrait>, Option<Int32Array>)>;

    fn explode(&self) -> Self::Output {
        use GeoDataType::*;

        macro_rules! call_explode {
            ($as_func:ident) => {{
                let (exploded_geoms, take_indices) = self.$as_func().explode();
                (Arc::new(exploded_geoms), take_indices)
            }};
        }

        let result: (Arc<dyn GeometryArrayTrait>, Option<Int32Array>) = match self.data_type() {
            Point(_, Dimension::XY) => call_explode!(as_point_2d),
            LineString(_, Dimension::XY) => call_explode!(as_line_string_2d),
            LargeLineString(_, Dimension::XY) => call_explode!(as_large_line_string_2d),
            Polygon(_, Dimension::XY) => call_explode!(as_polygon_2d),
            LargePolygon(_, Dimension::XY) => call_explode!(as_large_polygon_2d),
            MultiPoint(_, Dimension::XY) => call_explode!(as_multi_point_2d),
            LargeMultiPoint(_, Dimension::XY) => call_explode!(as_large_multi_point_2d),
            MultiLineString(_, Dimension::XY) => call_explode!(as_multi_line_string_2d),
            LargeMultiLineString(_, Dimension::XY) => call_explode!(as_large_multi_line_string_2d),
            MultiPolygon(_, Dimension::XY) => call_explode!(as_multi_polygon_2d),
            LargeMultiPolygon(_, Dimension::XY) => call_explode!(as_large_multi_polygon_2d),
            // Mixed(_, Dimension::XY) => self.as_mixed_2d().explode(),
            // LargeMixed(_, Dimension::XY) => self.as_large_mixed_2d().explode(),
            // GeometryCollection(_, Dimension::XY) => self.as_geometry_collection_2d().explode(),
            // LargeGeometryCollection(_, Dimension::XY) => self.as_large_geometry_collection_2d().explode(),
            _ => return Err(GeoArrowError::IncorrectType("".into())),
        };
        Ok(result)
    }
}

impl<G: GeometryArrayTrait> Explode for ChunkedGeometryArray<G> {
    type Output = Result<(
        Arc<dyn ChunkedGeometryArrayTrait>,
        Option<ChunkedArray<Int32Array>>,
    )>;

    fn explode(&self) -> Self::Output {
        let result = self.try_map(|chunk| chunk.as_ref().explode())?;

        // Convert Vec of tuples to tuple of vecs
        let (geometry_arrays, take_indices): (Vec<_>, Vec<_>) = result.into_iter().unzip();
        let geometry_array_refs = geometry_arrays
            .iter()
            .map(|x| x.as_ref())
            .collect::<Vec<_>>();

        // Convert Vec<Option<_>> to Option<Vec<_>>
        let take_indices: Option<Vec<_>> = take_indices.into_iter().collect();
        Ok((
            from_geoarrow_chunks(geometry_array_refs.as_slice())?,
            take_indices.map(ChunkedArray::new),
        ))
    }
}

impl Explode for &dyn ChunkedGeometryArrayTrait {
    type Output = Result<(
        Arc<dyn ChunkedGeometryArrayTrait>,
        Option<ChunkedArray<Int32Array>>,
    )>;

    fn explode(&self) -> Self::Output {
        match self.data_type() {
            GeoDataType::Point(_, Dimension::XY) => self.as_point_2d().explode(),
            GeoDataType::LineString(_, Dimension::XY) => self.as_line_string_2d().explode(),
            GeoDataType::LargeLineString(_, Dimension::XY) => {
                self.as_large_line_string_2d().explode()
            }
            GeoDataType::Polygon(_, Dimension::XY) => self.as_polygon_2d().explode(),
            GeoDataType::LargePolygon(_, Dimension::XY) => self.as_large_polygon_2d().explode(),
            GeoDataType::MultiPoint(_, Dimension::XY) => self.as_multi_point_2d().explode(),
            GeoDataType::LargeMultiPoint(_, Dimension::XY) => {
                self.as_large_multi_point_2d().explode()
            }
            GeoDataType::MultiLineString(_, Dimension::XY) => {
                self.as_multi_line_string_2d().explode()
            }
            GeoDataType::LargeMultiLineString(_, Dimension::XY) => {
                self.as_large_multi_line_string_2d().explode()
            }
            GeoDataType::MultiPolygon(_, Dimension::XY) => self.as_multi_polygon_2d().explode(),
            GeoDataType::LargeMultiPolygon(_, Dimension::XY) => {
                self.as_large_multi_polygon_2d().explode()
            }
            GeoDataType::Mixed(_, Dimension::XY) => self.as_mixed_2d().explode(),
            GeoDataType::LargeMixed(_, Dimension::XY) => self.as_large_mixed_2d().explode(),
            GeoDataType::GeometryCollection(_, Dimension::XY) => {
                self.as_geometry_collection_2d().explode()
            }
            GeoDataType::LargeGeometryCollection(_, Dimension::XY) => {
                self.as_large_geometry_collection_2d().explode()
            }
            GeoDataType::Rect(Dimension::XY) => self.as_rect_2d().explode(),
            _ => todo!(),
        }
    }
}

pub trait ExplodeTable {
    /// Returns the exploded geometries and, if an explode needs to happen, the indices that should
    /// be passed into a [`take`][arrow::compute::take] operation.
    fn explode(&self, index: Option<usize>) -> Result<Table>;
}

impl ExplodeTable for Table {
    fn explode(&self, index: Option<usize>) -> Result<Table> {
        let index = if let Some(index) = index {
            index
        } else {
            self.default_geometry_column_idx()?
        };

        let geometry_column = self.geometry_column(Some(index))?;
        let (exploded_geometry, take_indices) = geometry_column.as_ref().explode()?;

        // TODO: optionally use rayon?
        if let Some(take_indices) = take_indices {
            // Remove existing geometry column
            let mut new_table = self.clone();
            new_table.remove_column(index);

            let field = exploded_geometry.extension_field();

            // Call take on each chunk and append geometry chunk
            let new_batches = new_table
                .batches()
                .iter()
                .zip(take_indices.chunks())
                .zip(exploded_geometry.geometry_chunks())
                .map(|((batch, indices), geom_chunk)| {
                    let mut schema_builder = SchemaBuilder::from(batch.schema().as_ref().clone());

                    let mut new_columns = batch
                        .columns()
                        .iter()
                        .map(|values| Ok(take(values, indices, None)?))
                        .collect::<Result<Vec<_>>>()?;

                    // Add geometry column
                    new_columns.push(geom_chunk.to_array_ref());
                    schema_builder.push(field.clone());

                    Ok(RecordBatch::try_new(
                        schema_builder.finish().into(),
                        new_columns,
                    )?)
                })
                .collect::<Result<Vec<_>>>()?;

            // Update top-level schema
            let mut schema_builder = SchemaBuilder::from(new_table.schema().as_ref().clone());
            schema_builder.push(field.clone());
            let schema = schema_builder.finish();

            Table::try_new(schema.into(), new_batches)
        } else {
            // No take is necessary; nothing happens
            Ok(self.clone())
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::test::multipoint;
    use crate::trait_::GeometryArrayAccessor;

    #[test]
    fn explode_multi_point() {
        let arr = multipoint::mp_array();
        let (exploded_geoms, take_indices) = arr.explode();

        assert_eq!(exploded_geoms.value_as_geo(0), multipoint::mp0().0[0]);
        assert_eq!(exploded_geoms.value_as_geo(1), multipoint::mp0().0[1]);
        assert_eq!(exploded_geoms.value_as_geo(2), multipoint::mp1().0[0]);
        assert_eq!(exploded_geoms.value_as_geo(3), multipoint::mp1().0[1]);

        let take_indices = take_indices.unwrap();
        assert_eq!(take_indices.value(0), 0);
        assert_eq!(take_indices.value(1), 0);
        assert_eq!(take_indices.value(2), 1);
        assert_eq!(take_indices.value(3), 1);
    }
}
