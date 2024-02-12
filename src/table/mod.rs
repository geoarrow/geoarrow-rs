//! Abstractions for Arrow tables. Useful for dataset IO where data will have geometries and
//! attributes.

use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{FieldRef, SchemaBuilder, SchemaRef};

use crate::algorithm::native::Downcast;
use crate::array::*;
use crate::chunked_array::{from_arrow_chunks, from_geoarrow_chunks, ChunkedGeometryArrayTrait};
use crate::chunked_array::{ChunkedArray, ChunkedGeometryArray};
use crate::datatypes::GeoDataType;
use crate::error::{GeoArrowError, Result};
use crate::io::wkb::from_wkb;
use phf::{phf_set, Set};

static GEOARROW_EXTENSION_NAMES: Set<&'static str> = phf_set! {
    "geoarrow.point",
    "geoarrow.linestring",
    "geoarrow.polygon",
    "geoarrow.multipoint",
    "geoarrow.multilinestring",
    "geoarrow.multipolygon",
    "geoarrow.geometry",
    "geoarrow.geometrycollection",
    "geoarrow.wkb",
    "ogc.wkb",
};

#[derive(Debug, PartialEq, Clone)]
pub struct GeoTable {
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
    geometry_column_index: usize,
}

impl GeoTable {
    pub fn try_new(
        schema: SchemaRef,
        batches: Vec<RecordBatch>,
        geometry_column_index: usize,
    ) -> Result<Self> {
        // TODO: validate
        Ok(Self {
            schema,
            batches,
            geometry_column_index,
        })
    }

    pub fn from_arrow_and_geometry(
        batches: Vec<RecordBatch>,
        schema: SchemaRef,
        geometry: Arc<dyn ChunkedGeometryArrayTrait>,
    ) -> Result<Self> {
        if batches.is_empty() {
            return Err(GeoArrowError::General("empty input".to_string()));
        }

        let mut builder = SchemaBuilder::from(schema.fields());
        builder.push(geometry.extension_field());
        let new_schema = Arc::new(builder.finish());

        let mut new_batches = Vec::with_capacity(batches.len());
        for (batch, geometry_chunk) in batches.into_iter().zip(geometry.geometry_chunks()) {
            let mut columns = batch.columns().to_vec();
            columns.push(geometry_chunk.to_array_ref());
            new_batches.push(RecordBatch::try_new(new_schema.clone(), columns)?);
        }

        let geometry_column_index = new_schema.fields().len() - 1;
        Self::try_new(new_schema, new_batches, geometry_column_index)
    }

    // Note: This function is relatively complex because we want to parse any WKB columns to
    // geoarrow-native arrays
    pub fn from_arrow(
        batches: Vec<RecordBatch>,
        schema: SchemaRef,
        geometry_column_index: Option<usize>,
        target_geo_data_type: Option<GeoDataType>,
    ) -> Result<Self> {
        if batches.is_empty() {
            return Err(GeoArrowError::General("empty input".to_string()));
        }

        let num_batches = batches.len();

        let original_geometry_column_index = geometry_column_index.unwrap_or_else(|| {
            schema
                .fields
                .iter()
                .position(|field| {
                    field
                        .metadata()
                        .get("ARROW:extension:name")
                        .is_some_and(|extension_name| {
                            GEOARROW_EXTENSION_NAMES.contains(extension_name.as_str())
                        })
                })
                .expect("no geometry column in table")
        });

        let original_geometry_field = schema.field(original_geometry_column_index);

        let mut new_schema = SchemaBuilder::with_capacity(schema.fields().len());
        schema.fields().iter().enumerate().for_each(|(i, field)| {
            if i != original_geometry_column_index {
                new_schema.push(field.clone())
            }
        });

        let mut new_batches = Vec::with_capacity(num_batches);
        let mut orig_geom_chunks = Vec::with_capacity(num_batches);
        for batch in batches.into_iter() {
            let mut new_batch = Vec::with_capacity(batch.num_columns());
            for (i, col) in batch.columns().iter().enumerate() {
                if i != original_geometry_column_index {
                    new_batch.push(col.clone());
                } else {
                    orig_geom_chunks.push(col.clone());
                }
            }
            new_batches.push(new_batch);
        }

        let orig_geom_slices = orig_geom_chunks
            .iter()
            .map(|c| c.as_ref())
            .collect::<Vec<_>>();
        let mut chunked_geometry_array =
            from_arrow_chunks(orig_geom_slices.as_slice(), original_geometry_field)?;

        let target_geo_data_type =
            target_geo_data_type.unwrap_or(GeoDataType::LargeMixed(Default::default()));
        match chunked_geometry_array.data_type() {
            GeoDataType::WKB => {
                let parsed_chunks = chunked_geometry_array
                    .as_ref()
                    .as_wkb()
                    .chunks()
                    .iter()
                    .map(|chunk| from_wkb(chunk, target_geo_data_type, true))
                    .collect::<Result<Vec<_>>>()?;
                let parsed_chunks_refs = parsed_chunks
                    .iter()
                    .map(|chunk| chunk.as_ref())
                    .collect::<Vec<_>>();
                chunked_geometry_array = from_geoarrow_chunks(parsed_chunks_refs.as_slice())?
                    .as_ref()
                    .downcast(true);
            }
            GeoDataType::LargeWKB => {
                let parsed_chunks = chunked_geometry_array
                    .as_ref()
                    .as_large_wkb()
                    .chunks()
                    .iter()
                    .map(|chunk| from_wkb(chunk, target_geo_data_type, true))
                    .collect::<Result<Vec<_>>>()?;
                let parsed_chunks_refs = parsed_chunks
                    .iter()
                    .map(|chunk| chunk.as_ref())
                    .collect::<Vec<_>>();
                chunked_geometry_array = from_geoarrow_chunks(parsed_chunks_refs.as_slice())?
                    .as_ref()
                    .downcast(true);
            }
            _ => (),
        };

        new_schema.push(chunked_geometry_array.extension_field());
        let new_schema = Arc::new(new_schema.finish());
        let new_geometry_column_index = new_schema.fields().len() - 1;

        let mut new_record_batches = Vec::with_capacity(num_batches);
        for (mut new_batch, geom_chunk) in new_batches
            .into_iter()
            .zip(chunked_geometry_array.geometry_chunks())
        {
            new_batch.push(geom_chunk.to_array_ref());
            new_record_batches.push(RecordBatch::try_new(new_schema.clone(), new_batch).unwrap());
        }

        GeoTable::try_new(new_schema, new_record_batches, new_geometry_column_index)
    }

    pub fn len(&self) -> usize {
        self.batches.iter().fold(0, |sum, val| sum + val.num_rows())
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn into_inner(self) -> (SchemaRef, Vec<RecordBatch>, usize) {
        (self.schema, self.batches, self.geometry_column_index)
    }

    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    pub fn batches(&self) -> &Vec<RecordBatch> {
        &self.batches
    }

    pub fn geometry_column_index(&self) -> usize {
        self.geometry_column_index
    }

    pub fn geometry_data_type(&self) -> Result<GeoDataType> {
        Ok(*self.geometry()?.data_type())
    }

    /// The number of columns in this table.
    pub fn num_columns(&self) -> usize {
        self.schema.fields().len()
    }

    pub(crate) fn remove_column(&mut self, i: usize) -> ChunkedArray<ArrayRef> {
        // NOTE: remove_column drops schema metadata as of
        // https://github.com/apache/arrow-rs/issues/5327
        let removed_chunks = self
            .batches
            .iter_mut()
            .map(|batch| batch.remove_column(i))
            .collect::<Vec<_>>();

        let mut schema_builder = SchemaBuilder::from(self.schema.as_ref().clone());
        schema_builder.remove(i);
        self.schema = Arc::new(schema_builder.finish());

        ChunkedArray::new(removed_chunks)
    }

    #[allow(dead_code)]
    pub(crate) fn append_column(
        &mut self,
        field: FieldRef,
        column: ChunkedArray<ArrayRef>,
    ) -> Result<usize> {
        assert_eq!(self.batches().len(), column.chunks().len());

        let new_batches = self
            .batches
            .iter_mut()
            .zip(column.chunks)
            .map(|(batch, array)| {
                let mut schema_builder = SchemaBuilder::from(batch.schema().as_ref().clone());
                schema_builder.push(field.clone());

                let mut columns = batch.columns().to_vec();
                columns.push(array);
                Ok(RecordBatch::try_new(
                    schema_builder.finish().into(),
                    columns,
                )?)
                // let schema = batch.schema()
            })
            .collect::<Result<Vec<_>>>()?;

        self.batches = new_batches;

        let mut schema_builder = SchemaBuilder::from(self.schema.as_ref().clone());
        schema_builder.push(field.clone());
        self.schema = schema_builder.finish().into();

        Ok(self.schema.fields().len() - 1)
    }

    /// Access the geometry column of the table
    pub fn geometry(&self) -> Result<Arc<dyn ChunkedGeometryArrayTrait>> {
        let field = self.schema.field(self.geometry_column_index);
        let array_refs = self
            .batches
            .iter()
            .map(|batch| batch.column(self.geometry_column_index))
            .collect::<Vec<_>>();
        let geo_data_type = GeoDataType::try_from(field)?;
        match geo_data_type {
            GeoDataType::Point(_) => {
                let chunks: Result<Vec<PointArray>> = array_refs
                    .into_iter()
                    .map(|arr| arr.as_ref().try_into())
                    .collect();
                Ok(Arc::new(ChunkedGeometryArray::new(chunks?)))
            }
            GeoDataType::LineString(_) => {
                let chunks: Result<Vec<LineStringArray<i32>>> = array_refs
                    .into_iter()
                    .map(|arr| arr.as_ref().try_into())
                    .collect();
                Ok(Arc::new(ChunkedGeometryArray::new(chunks?)))
            }
            GeoDataType::LargeLineString(_) => {
                let chunks: Result<Vec<LineStringArray<i64>>> = array_refs
                    .into_iter()
                    .map(|arr| arr.as_ref().try_into())
                    .collect();
                Ok(Arc::new(ChunkedGeometryArray::new(chunks?)))
            }
            GeoDataType::Polygon(_) => {
                let chunks: Result<Vec<PolygonArray<i32>>> = array_refs
                    .into_iter()
                    .map(|arr| arr.as_ref().try_into())
                    .collect();
                Ok(Arc::new(ChunkedGeometryArray::new(chunks?)))
            }
            GeoDataType::LargePolygon(_) => {
                let chunks: Result<Vec<PolygonArray<i64>>> = array_refs
                    .into_iter()
                    .map(|arr| arr.as_ref().try_into())
                    .collect();
                Ok(Arc::new(ChunkedGeometryArray::new(chunks?)))
            }
            GeoDataType::MultiPoint(_) => {
                let chunks: Result<Vec<MultiPointArray<i32>>> = array_refs
                    .into_iter()
                    .map(|arr| arr.as_ref().try_into())
                    .collect();
                Ok(Arc::new(ChunkedGeometryArray::new(chunks?)))
            }
            GeoDataType::LargeMultiPoint(_) => {
                let chunks: Result<Vec<MultiPointArray<i64>>> = array_refs
                    .into_iter()
                    .map(|arr| arr.as_ref().try_into())
                    .collect();
                Ok(Arc::new(ChunkedGeometryArray::new(chunks?)))
            }
            GeoDataType::MultiLineString(_) => {
                let chunks: Result<Vec<MultiLineStringArray<i32>>> = array_refs
                    .into_iter()
                    .map(|arr| arr.as_ref().try_into())
                    .collect();
                Ok(Arc::new(ChunkedGeometryArray::new(chunks?)))
            }
            GeoDataType::LargeMultiLineString(_) => {
                let chunks: Result<Vec<MultiLineStringArray<i64>>> = array_refs
                    .into_iter()
                    .map(|arr| arr.as_ref().try_into())
                    .collect();
                Ok(Arc::new(ChunkedGeometryArray::new(chunks?)))
            }
            GeoDataType::MultiPolygon(_) => {
                let chunks: Result<Vec<MultiPolygonArray<i32>>> = array_refs
                    .into_iter()
                    .map(|arr| arr.as_ref().try_into())
                    .collect();
                Ok(Arc::new(ChunkedGeometryArray::new(chunks?)))
            }
            GeoDataType::LargeMultiPolygon(_) => {
                let chunks: Result<Vec<MultiPolygonArray<i64>>> = array_refs
                    .into_iter()
                    .map(|arr| arr.as_ref().try_into())
                    .collect();
                Ok(Arc::new(ChunkedGeometryArray::new(chunks?)))
            }
            GeoDataType::Mixed(_) => {
                let chunks: Result<Vec<MixedGeometryArray<i32>>> = array_refs
                    .into_iter()
                    .map(|arr| arr.as_ref().try_into())
                    .collect();
                Ok(Arc::new(ChunkedGeometryArray::new(chunks?)))
            }
            GeoDataType::LargeMixed(_) => {
                let chunks: Result<Vec<MixedGeometryArray<i64>>> = array_refs
                    .into_iter()
                    .map(|arr| arr.as_ref().try_into())
                    .collect();
                Ok(Arc::new(ChunkedGeometryArray::new(chunks?)))
            }
            GeoDataType::GeometryCollection(_) => {
                let chunks: Result<Vec<GeometryCollectionArray<i32>>> = array_refs
                    .into_iter()
                    .map(|arr| arr.as_ref().try_into())
                    .collect();
                Ok(Arc::new(ChunkedGeometryArray::new(chunks?)))
            }
            GeoDataType::LargeGeometryCollection(_) => {
                let chunks: Result<Vec<GeometryCollectionArray<i64>>> = array_refs
                    .into_iter()
                    .map(|arr| arr.as_ref().try_into())
                    .collect();
                Ok(Arc::new(ChunkedGeometryArray::new(chunks?)))
            }
            GeoDataType::WKB => {
                let chunks: Result<Vec<WKBArray<i32>>> = array_refs
                    .into_iter()
                    .map(|arr| arr.as_ref().try_into())
                    .collect();
                Ok(Arc::new(ChunkedGeometryArray::new(chunks?)))
            }
            GeoDataType::LargeWKB => {
                let chunks: Result<Vec<WKBArray<i64>>> = array_refs
                    .into_iter()
                    .map(|arr| arr.as_ref().try_into())
                    .collect();
                Ok(Arc::new(ChunkedGeometryArray::new(chunks?)))
            }
            GeoDataType::Rect => {
                // tryfrom not implemented for RectArray
                todo!()
                // let chunks: Result<Vec<RectArray>> = array_refs
                //     .into_iter()
                //     .map(|arr| arr.as_ref().try_into())
                //     .collect();
                // Ok(Arc::new(ChunkedGeometryArray::new(chunks?)))
            }
        }
    }
}
