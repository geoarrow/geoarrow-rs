use std::sync::Arc;

use arrow::compute::take;
use arrow_array::{Int32Array, OffsetSizeTrait, RecordBatch};
use arrow_buffer::OffsetBuffer;
use arrow_schema::SchemaBuilder;

use crate::array::*;
use crate::chunked_array::{ChunkedArray, ChunkedGeometryArray, ChunkedNativeArray, ChunkedNativeArrayDyn};
use crate::datatypes::{Dimension, NativeType};
use crate::error::{GeoArrowError, Result};
use crate::table::Table;
use crate::NativeArray;

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

impl Explode for LineStringArray<2> {
    type Output = (Self, Option<Int32Array>);

    fn explode(&self) -> Self::Output {
        (self.clone(), None)
    }
}

impl Explode for PolygonArray<2> {
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
fn explode_offsets(offsets: &OffsetBuffer<i32>) -> Int32Array {
    let mut take_indices: Vec<i32> = Vec::with_capacity(offsets.last().unwrap().to_usize().unwrap());
    for (offset_idx, offset_start_end) in offsets.as_ref().windows(2).enumerate() {
        let offset_start = offset_start_end[0].to_usize().unwrap();
        let offset_end = offset_start_end[1].to_usize().unwrap();
        for _ in offset_start..offset_end {
            take_indices.push(offset_idx.try_into().unwrap());
        }
    }
    Int32Array::new(take_indices.into(), None)
}

impl Explode for MultiPointArray<2> {
    type Output = (PointArray<2>, Option<Int32Array>);

    fn explode(&self) -> Self::Output {
        assert_eq!(self.null_count(), 0, "Null values not yet supported in explode");

        let exploded_geoms = PointArray::new(self.coords.clone(), None, self.metadata());
        let take_indices = explode_offsets(self.geom_offsets());
        (exploded_geoms, Some(take_indices))
    }
}

impl Explode for MultiLineStringArray<2> {
    type Output = (LineStringArray<2>, Option<Int32Array>);

    fn explode(&self) -> Self::Output {
        assert_eq!(self.null_count(), 0, "Null values not yet supported in explode");

        let exploded_geoms = LineStringArray::new(self.coords.clone(), self.ring_offsets.clone(), None, self.metadata());
        let take_indices = explode_offsets(self.geom_offsets());
        (exploded_geoms, Some(take_indices))
    }
}

impl Explode for MultiPolygonArray<2> {
    type Output = (PolygonArray<2>, Option<Int32Array>);

    fn explode(&self) -> Self::Output {
        assert_eq!(self.null_count(), 0, "Null values not yet supported in explode");

        let exploded_geoms = PolygonArray::new(self.coords.clone(), self.polygon_offsets.clone(), self.ring_offsets.clone(), None, self.metadata());
        let take_indices = explode_offsets(self.geom_offsets());
        (exploded_geoms, Some(take_indices))
    }
}

impl Explode for &dyn NativeArray {
    type Output = Result<(Arc<dyn NativeArray>, Option<Int32Array>)>;

    fn explode(&self) -> Self::Output {
        macro_rules! call_explode {
            ($as_func:ident) => {{
                let (exploded_geoms, take_indices) = self.$as_func().explode();
                (Arc::new(exploded_geoms), take_indices)
            }};
        }

        use Dimension::*;
        use NativeType::*;

        let result: (Arc<dyn NativeArray>, Option<Int32Array>) = match self.data_type() {
            Point(_, XY) => call_explode!(as_point),
            LineString(_, XY) => call_explode!(as_line_string),
            Polygon(_, XY) => call_explode!(as_polygon),
            MultiPoint(_, XY) => call_explode!(as_multi_point),
            MultiLineString(_, XY) => call_explode!(as_multi_line_string),
            MultiPolygon(_, XY) => call_explode!(as_multi_polygon),
            // Mixed(_, XY) => self.as_mixed::<2>().explode(),
            // GeometryCollection(_, XY) => self.as_geometry_collection::<2>().explode(),
            _ => return Err(GeoArrowError::IncorrectType("".into())),
        };
        Ok(result)
    }
}

impl<G: NativeArray> Explode for ChunkedGeometryArray<G> {
    type Output = Result<(Arc<dyn ChunkedNativeArray>, Option<ChunkedArray<Int32Array>>)>;

    fn explode(&self) -> Self::Output {
        let result = self.try_map(|chunk| chunk.as_ref().explode())?;

        // Convert Vec of tuples to tuple of vecs
        let (geometry_arrays, take_indices): (Vec<_>, Vec<_>) = result.into_iter().unzip();
        let geometry_array_refs = geometry_arrays.iter().map(|x| x.as_ref()).collect::<Vec<_>>();

        // Convert Vec<Option<_>> to Option<Vec<_>>
        let take_indices: Option<Vec<_>> = take_indices.into_iter().collect();
        Ok((ChunkedNativeArrayDyn::from_geoarrow_chunks(geometry_array_refs.as_slice())?.into_inner(), take_indices.map(ChunkedArray::new)))
    }
}

impl Explode for &dyn ChunkedNativeArray {
    type Output = Result<(Arc<dyn ChunkedNativeArray>, Option<ChunkedArray<Int32Array>>)>;

    fn explode(&self) -> Self::Output {
        use Dimension::*;
        use NativeType::*;

        match self.data_type() {
            Point(_, XY) => self.as_point::<2>().explode(),
            LineString(_, XY) => self.as_line_string::<2>().explode(),
            Polygon(_, XY) => self.as_polygon::<2>().explode(),
            MultiPoint(_, XY) => self.as_multi_point::<2>().explode(),
            MultiLineString(_, XY) => self.as_multi_line_string::<2>().explode(),
            MultiPolygon(_, XY) => self.as_multi_polygon::<2>().explode(),
            Mixed(_, XY) => self.as_mixed::<2>().explode(),
            GeometryCollection(_, XY) => self.as_geometry_collection::<2>().explode(),
            Rect(XY) => self.as_rect::<2>().explode(),
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
        let index = if let Some(index) = index { index } else { self.default_geometry_column_idx()? };

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

                    let mut new_columns = batch.columns().iter().map(|values| Ok(take(values, indices, None)?)).collect::<Result<Vec<_>>>()?;

                    // Add geometry column
                    new_columns.push(geom_chunk.to_array_ref());
                    schema_builder.push(field.clone());

                    Ok(RecordBatch::try_new(schema_builder.finish().into(), new_columns)?)
                })
                .collect::<Result<Vec<_>>>()?;

            // Update top-level schema
            let mut schema_builder = SchemaBuilder::from(new_table.schema().as_ref().clone());
            schema_builder.push(field.clone());
            let schema = schema_builder.finish();

            Table::try_new(new_batches, schema.into())
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
    use crate::trait_::ArrayAccessor;

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
