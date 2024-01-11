//! Abstractions for Arrow tables. Useful for dataset IO where data will have geometries and
//! attributes.

use std::sync::Arc;

use arrow_array::{Array, RecordBatch};
use arrow_schema::{Field, SchemaBuilder, SchemaRef};

use crate::algorithm::native::Downcast;
use crate::array::*;
use crate::chunked_array::chunked_array::{
    from_arrow_chunks, from_geoarrow_chunks, ChunkedGeometryArrayTrait,
};
use crate::chunked_array::ChunkedGeometryArray;
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
pub struct Table {
    /// The schema of each batch within the table. This schema must include extension metadata for
    /// any geometry columns.
    schema: SchemaRef,

    /// A list of [RecordBatch] objects, which store the actual table data.
    batches: Vec<RecordBatch>,

    /// The positional index of the primary geometry column.
    ///
    /// - This can be `None` if there is no geometry column in the table.
    /// - If a single geometry column exists in the table, this **MUST** be set.
    /// - If more than one geometry column exists in the table, this **MAY** not be set. In that
    ///   case, the user must pass in the index of the geometry column they wish to access.
    primary_geometry_idx: Option<usize>,
}

impl Table {
    pub fn try_new(
        schema: SchemaRef,
        batches: Vec<RecordBatch>,
        primary_geometry_idx: Option<usize>,
    ) -> Result<Self> {
        // TODO: validate
        Ok(Self {
            schema,
            batches,
            primary_geometry_idx,
        })
    }

    /// Remove a column from the table
    pub fn remove_column(&mut self, idx: usize) {}

    /// Append a column to this table.
    pub fn append_column(&mut self, chunks: Vec<Arc<dyn Array>>, field: Arc<Field>) {}

    /// Replace the column at index `idx` with new data
    pub fn replace_column(
        &mut self,
        idx: usize,
        chunks: Vec<Arc<dyn Array>>,
        field: Arc<Field>,
    ) {
        let schema = self.schema.borrow_mut();
        schema.fields
        todo!()
    }

    /// Construct a Table from an external Arrow source
    ///
    /// This schema is a **descriptive** schema, not a **prescriptive** one. Casts will not be
    /// taken into account.
    ///
    /// `geometry_columns` can be used to prescribe
    ///
    /// The geometry columns are the exception to this.
    ///
    // Note: This function is relatively complex because we want to parse any WKB columns to
    // geoarrow-native arrays
    pub fn from_arrow(
        batches: Vec<RecordBatch>,
        schema: SchemaRef,
        geometry_columns: Option<&[(usize, Option<GeoDataType>)]>,
    ) -> Result<Self> {
        if batches.is_empty() {
            return Err(GeoArrowError::General("empty input".to_string()));
        }

        let num_batches = batches.len();

        // - If `geometry_columns` is passed:
        //     - For each index, if GeoDataType exists, cast to that. Otherwise cast to LargeMixed.
        // - If `geometry_columns` is `None` but fields exist with geoarrow metadata, parse

        // Requirements:
        // - enable Cast to handle from wkb to a type
        // - Clean up this function by using `replace_column`

        // -

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

        let mut chunked_geometry_array =
            from_arrow_chunks(orig_geom_chunks.as_slice(), original_geometry_field)?;

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

    pub fn into_inner(self) -> (SchemaRef, Vec<RecordBatch>, Option<usize>) {
        (self.schema, self.batches, self.primary_geometry_idx)
    }

    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    pub fn batches(&self) -> &Vec<RecordBatch> {
        &self.batches
    }

    /// The index of the primary geometry column, if set.
    pub fn primary_geometry_idx(&self) -> Option<usize> {
        self.primary_geometry_idx
    }

    pub fn geometry_data_type(&self, index: Option<usize>) -> Result<GeoDataType> {
        Ok(*self.geometry(index)?.data_type())
    }

    /// The number of columns in this table.
    pub fn num_columns(&self) -> usize {
        self.schema.fields().len()
    }

    /// Access the geometry column of the table
    pub fn geometry(&self, index: Option<usize>) -> Result<Arc<dyn ChunkedGeometryArrayTrait>> {
        let field = self.schema.field(self.geometry_column_index);
        let array_refs = self
            .batches
            .iter()
            .map(|batch| batch.column(self.geometry_column_index))
            .collect::<Vec<_>>();
        from_arrow_chunks(array_refs.as_slice(), field)
    }
}
