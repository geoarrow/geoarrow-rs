//! Abstractions for Arrow tables. Useful for dataset IO where data will have geometries and
//! attributes.

use std::ops::Deref;
use std::sync::Arc;

use arrow_array::{Array, ArrayRef, RecordBatch};
use arrow_schema::{ArrowError, FieldRef, Schema, SchemaBuilder, SchemaRef};

use crate::algorithm::native::{Cast, Downcast};
use crate::array::*;
use crate::chunked_array::ChunkedArray;
use crate::chunked_array::{from_arrow_chunks, from_geoarrow_chunks, ChunkedGeometryArrayTrait};
use crate::datatypes::GeoDataType;
use crate::error::{GeoArrowError, Result};
use crate::io::wkb::from_wkb;
use phf::{phf_set, Set};

pub(crate) static GEOARROW_EXTENSION_NAMES: Set<&'static str> = phf_set! {
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

/// An Arrow table that MAY contain one or more geospatial columns.
///
/// This Table object is designed to be interoperable with non-geospatial Arrow libraries, and thus
/// does not _require_ a geometry column.
#[derive(Debug, PartialEq, Clone)]
pub struct Table {
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
}

impl Table {
    pub fn try_new(schema: SchemaRef, batches: Vec<RecordBatch>) -> Result<Self> {
        for batch in batches.iter() {
            // Don't check schema metadata in comparisons.
            // TODO: I have some issues in the Parquet reader where the batches are missing the
            // schema metadata.
            if batch.schema().fields() != schema.fields() {
                return Err(GeoArrowError::General(format!(
                    "Schema is not consistent across batches. Expected {}, got {}. With expected metadata: {:?}, got {:?}",
                    schema,
                    batch.schema(),
                    schema.metadata(),
                    batch.schema().metadata()
                )));
            }
        }

        Ok(Self { schema, batches })
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

        Self::try_new(new_schema, new_batches)
    }

    /// Cast the geometry at `index` to a different data type
    pub fn cast_geometry(&mut self, index: usize, to_type: &GeoDataType) -> Result<()> {
        let orig_field = self.schema().field(index);

        let array_slices = self
            .batches()
            .iter()
            .map(|batch| batch.column(index).as_ref())
            .collect::<Vec<_>>();
        let chunked_geometry = from_arrow_chunks(array_slices.as_slice(), orig_field)?;
        let casted_geometry = chunked_geometry.as_ref().cast(to_type)?;
        let casted_arrays = casted_geometry.array_refs();
        let casted_field = to_type.to_field(orig_field.name(), orig_field.is_nullable());

        self.set_column(index, casted_field.into(), casted_arrays)?;

        Ok(())
    }

    /// Parse the geometry at `index` to a GeoArrow-native type
    ///
    /// Use [Self::cast_geometry] if you know the target data type
    pub fn parse_geometry_to_native(
        &mut self,
        index: usize,
        target_geo_data_type: Option<GeoDataType>,
    ) -> Result<()> {
        let target_geo_data_type =
            target_geo_data_type.unwrap_or(GeoDataType::LargeMixed(Default::default()));
        let orig_field = self.schema().field(index);

        // If the table is empty, don't try to parse WKB column
        // An empty column will crash currently in `from_arrow_chunks` or alternatively
        // `chunked_geometry.data_type`.
        if self.is_empty() {
            let new_field =
                target_geo_data_type.to_field(orig_field.name(), orig_field.is_nullable());
            let new_arrays = vec![];
            self.set_column(index, new_field.into(), new_arrays)?;
            return Ok(());
        }

        let array_slices = self
            .batches()
            .iter()
            .map(|batch| batch.column(index).as_ref())
            .collect::<Vec<_>>();
        let chunked_geometry = from_arrow_chunks(array_slices.as_slice(), orig_field)?;

        // Parse WKB
        let new_geometry = match chunked_geometry.data_type() {
            GeoDataType::WKB => {
                let parsed_chunks = chunked_geometry
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
                from_geoarrow_chunks(parsed_chunks_refs.as_slice())?
                    .as_ref()
                    .downcast(true)
            }
            GeoDataType::LargeWKB => {
                let parsed_chunks = chunked_geometry
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
                from_geoarrow_chunks(parsed_chunks_refs.as_slice())?
                    .as_ref()
                    .downcast(true)
            }
            _ => chunked_geometry,
        };

        let new_field = new_geometry
            .data_type()
            .to_field(orig_field.name(), orig_field.is_nullable());
        let new_arrays = new_geometry.array_refs();

        self.set_column(index, new_field.into(), new_arrays)?;

        Ok(())
    }

    // Note: This function is relatively complex because we want to parse any WKB columns to
    // geoarrow-native arrays
    #[deprecated]
    pub fn from_arrow(
        batches: Vec<RecordBatch>,
        schema: SchemaRef,
        geometry_column_index: Option<usize>,
        target_geo_data_type: Option<GeoDataType>,
    ) -> Result<Self> {
        if batches.is_empty() {
            return Self::try_new(schema, batches);
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

        let mut new_record_batches = Vec::with_capacity(num_batches);
        for (mut new_batch, geom_chunk) in new_batches
            .into_iter()
            .zip(chunked_geometry_array.geometry_chunks())
        {
            new_batch.push(geom_chunk.to_array_ref());
            new_record_batches.push(RecordBatch::try_new(new_schema.clone(), new_batch).unwrap());
        }

        Table::try_new(new_schema, new_record_batches)
    }

    pub fn len(&self) -> usize {
        self.batches.iter().fold(0, |sum, val| sum + val.num_rows())
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn into_inner(self) -> (SchemaRef, Vec<RecordBatch>) {
        (self.schema, self.batches)
    }

    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    pub fn batches(&self) -> &[RecordBatch] {
        &self.batches
    }

    /// Find the indices of all geometry columns in this table.
    ///
    /// This may be an empty Vec if the table contains no geometry columns, or a vec with more than
    /// one element if the table contains multiple tagged geometry columns.

    // TODO: this should really be on a Schema object instead.
    pub fn geometry_column_indices(&self) -> Vec<usize> {
        let mut geom_indices = vec![];
        for (field_idx, field) in self.schema().fields().iter().enumerate() {
            let meta = field.metadata();
            if let Some(ext_name) = meta.get("ARROW:extension:name") {
                if GEOARROW_EXTENSION_NAMES.contains(ext_name.as_str()) {
                    geom_indices.push(field_idx);
                }
            }
        }
        geom_indices
    }

    pub fn default_geometry_column_idx(&self) -> Result<usize> {
        let geom_col_indices = self.geometry_column_indices();
        if geom_col_indices.len() != 1 {
            Err(GeoArrowError::General(
                "Cannot use default geometry column when multiple geometry columns exist in table"
                    .to_string(),
            ))
        } else {
            Ok(geom_col_indices[0])
        }
    }

    /// Access the geometry chunked array at the provided column index.
    pub fn geometry_column(
        &self,
        index: Option<usize>,
    ) -> Result<Arc<dyn ChunkedGeometryArrayTrait>> {
        let index = if let Some(index) = index {
            index
        } else {
            let geom_indices = self.geometry_column_indices();
            if geom_indices.len() == 1 {
                geom_indices[0]
            } else {
                return Err(GeoArrowError::General(
                    "`index` must be provided when multiple geometry columns exist.".to_string(),
                ));
            }
        };

        let field = self.schema.field(index);
        let array_refs = self
            .batches
            .iter()
            .map(|batch| batch.column(index).as_ref())
            .collect::<Vec<_>>();
        from_arrow_chunks(array_refs.as_slice(), field)
    }

    /// Access all geometry chunked arrays from the table.
    ///
    /// This may return an empty `Vec` if there are no geometry columns in the table, or may return
    /// more than one element if there are multiple geometry columns.
    pub fn geometry_columns(&self) -> Result<Vec<Arc<dyn ChunkedGeometryArrayTrait>>> {
        self.geometry_column_indices()
            .into_iter()
            .map(|index| self.geometry_column(Some(index)))
            .collect()
    }

    /// The number of columns in this table.
    pub fn num_columns(&self) -> usize {
        self.schema.fields().len()
    }

    /// Replace the column at index `i` with the given field and arrays.
    pub fn set_column(
        &mut self,
        i: usize,
        field: FieldRef,
        column: Vec<Arc<dyn Array>>,
    ) -> Result<()> {
        let mut fields = self.schema().fields().deref().to_vec();
        fields[i] = field;
        let schema = Arc::new(Schema::new_with_metadata(
            fields,
            self.schema().metadata().clone(),
        ));

        let batches = self
            .batches
            .iter()
            .zip(column)
            .map(|(batch, array)| {
                let mut arrays = batch.columns().to_vec();
                arrays[i] = array;
                RecordBatch::try_new(schema.clone(), arrays)
            })
            .collect::<std::result::Result<Vec<_>, ArrowError>>()?;

        self.schema = schema;
        self.batches = batches;

        Ok(())
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

    pub fn append_column(&mut self, field: FieldRef, column: Vec<Arc<dyn Array>>) -> Result<usize> {
        assert_eq!(self.batches().len(), column.len());

        let new_batches = self
            .batches
            .iter_mut()
            .zip(column)
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
}
