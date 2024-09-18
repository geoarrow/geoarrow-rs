//! Abstractions for Arrow tables.
//!
//! Useful for dataset IO where data will have geometries and attributes.

use std::ops::Deref;
use std::sync::Arc;

use arrow_array::{Array, ArrayRef, RecordBatch, RecordBatchIterator, RecordBatchReader};
use arrow_schema::{ArrowError, FieldRef, Schema, SchemaBuilder, SchemaRef};

use crate::algorithm::native::{Cast, Downcast};
use crate::array::metadata::ArrayMetadata;
use crate::array::*;
use crate::chunked_array::ChunkedArray;
use crate::chunked_array::{from_arrow_chunks, from_geoarrow_chunks, ChunkedNativeArray};
use crate::datatypes::{AnyType, Dimension, NativeType, SerializedType};
use crate::error::{GeoArrowError, Result};
use crate::io::wkb::from_wkb;
use crate::schema::GeoSchemaExt;
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
    /// Creates a new table from a schema and a vector of record batches.
    ///
    /// # Errors
    ///
    /// Returns an error if a record batch's schema fields do not match the
    /// top-level schema's fields.
    ///
    /// # Examples
    ///
    /// ```
    /// use arrow_array::RecordBatch;
    /// use arrow_schema::{Schema, SchemaRef};
    /// use geoarrow::{NativeArray, array::PointArray, table::Table};
    ///
    /// let point = geo::point!(x: 1., y: 2.);
    /// let array: PointArray<2> = vec![point].as_slice().into();
    /// let field = array.extension_field();
    /// let schema: SchemaRef = Schema::new(vec![field]).into();
    /// let columns = vec![array.into_array_ref()];
    /// let batch = RecordBatch::try_new(schema.clone(), columns).unwrap();
    /// let table = Table::try_new(vec![batch], schema).unwrap();
    /// ```
    pub fn try_new(batches: Vec<RecordBatch>, schema: SchemaRef) -> Result<Self> {
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

        Ok(Self { batches, schema })
    }

    /// Creates a new table from a schema, a vector of record batches, and a chunked geometry array.
    ///
    /// # Errors
    ///
    /// Returns an error if a record batch's schema fields do not match the
    /// top-level schema's fields, or if the batches are empty.
    ///
    /// # Examples
    ///
    /// ```
    /// use arrow_array::{Int32Array, RecordBatch};
    /// use arrow_schema::{DataType, Schema, SchemaRef, Field};
    /// use geoarrow::{
    ///     NativeArray,
    ///     array::PointArray,
    ///     table::Table,
    ///     chunked_array::ChunkedGeometryArray
    /// };
    /// use std::sync::Arc;
    ///
    /// let point = geo::point!(x: 1., y: 2.);
    /// let array: PointArray<2> = vec![point].as_slice().into();
    /// let chunked_array = ChunkedGeometryArray::new(vec![array]);
    ///
    /// let id_array = Int32Array::from(vec![1]);
    /// let schema_ref = Arc::new(Schema::new(vec![
    ///     Field::new("id", DataType::Int32, false)
    /// ]));
    /// let batch = RecordBatch::try_new(
    ///     schema_ref.clone(),
    ///     vec![Arc::new(id_array)]
    /// ).unwrap();
    ///
    /// let table = Table::from_arrow_and_geometry(vec![batch], schema_ref, Arc::new(chunked_array)).unwrap();
    /// ```
    pub fn from_arrow_and_geometry(
        batches: Vec<RecordBatch>,
        schema: SchemaRef,
        geometry: Arc<dyn ChunkedNativeArray>,
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

        Self::try_new(new_batches, new_schema)
    }

    /// Casts the geometry at `index` to a different data type
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "geozero")]
    /// # {
    /// use std::fs::File;
    /// use geoarrow::{array::CoordType, datatypes::{NativeType, Dimension}};
    ///
    /// let file = File::open("fixtures/roads.geojson").unwrap();
    /// let mut table = geoarrow::io::geojson::read_geojson(file, Default::default()).unwrap();
    /// let index = table.default_geometry_column_idx().unwrap();
    ///
    /// // Change to separated storage of coordinates
    /// table.cast_geometry(index, &NativeType::LineString(CoordType::Separated, Dimension::XY)).unwrap();
    /// # }
    /// ```
    pub fn cast_geometry(&mut self, index: usize, to_type: &NativeType) -> Result<()> {
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

    /// Parse the serialized geometry at `index` to a GeoArrow-native type.
    ///
    /// Use [Self::cast_geometry] if you know the target data type.
    ///
    /// # Examples
    ///
    /// TODO
    pub fn parse_serialized_geometry(
        &self,
        index: usize,
        target_geo_data_type: Option<NativeType>,
    ) -> Result<Self> {
        // TODO: don't always default to XY
        let target_geo_data_type = target_geo_data_type
            .unwrap_or(NativeType::LargeMixed(Default::default(), Dimension::XY));

        let orig_field = self.schema().field(index);
        let geoarray_metadata = ArrayMetadata::try_from(orig_field)?;

        // If the table is empty, don't try to parse WKB column
        // An empty column will crash currently in `from_arrow_chunks` or alternatively
        // `chunked_geometry.data_type`.
        if self.is_empty() {
            let mut new_table = self.clone();
            let new_field = target_geo_data_type.to_field_with_metadata(
                orig_field.name(),
                orig_field.is_nullable(),
                &geoarray_metadata,
            );
            let new_arrays = vec![];
            new_table.set_column(index, new_field.into(), new_arrays)?;
            return Ok(new_table);
        }

        let orig_type = AnyType::try_from(orig_field)?;
        match orig_type {
            AnyType::Native(_) => Ok(self.clone()),
            AnyType::Serialized(typ) => {
                let array_slices = self
                    .batches()
                    .iter()
                    .map(|batch| batch.column(index).as_ref())
                    .collect::<Vec<_>>();
                let new_geometry = match typ {
                    SerializedType::WKB => {
                        let wkb_chunks = array_slices
                            .iter()
                            .map(|arr| WKBArray::<i32>::try_from((*arr, orig_field)))
                            .collect::<Result<Vec<_>>>()?;
                        let parsed_chunks = wkb_chunks
                            .into_iter()
                            .map(|chunk| from_wkb(&chunk, target_geo_data_type, true))
                            .collect::<Result<Vec<_>>>()?;
                        let parsed_chunks_refs = parsed_chunks
                            .iter()
                            .map(|chunk| chunk.as_ref())
                            .collect::<Vec<_>>();
                        from_geoarrow_chunks(parsed_chunks_refs.as_slice())?
                            .as_ref()
                            .downcast(true)
                    }
                    SerializedType::LargeWKB => {
                        let wkb_chunks = array_slices
                            .iter()
                            .map(|arr| WKBArray::<i64>::try_from((*arr, orig_field)))
                            .collect::<Result<Vec<_>>>()?;
                        let parsed_chunks = wkb_chunks
                            .into_iter()
                            .map(|chunk| from_wkb(&chunk, target_geo_data_type, true))
                            .collect::<Result<Vec<_>>>()?;
                        let parsed_chunks_refs = parsed_chunks
                            .iter()
                            .map(|chunk| chunk.as_ref())
                            .collect::<Vec<_>>();
                        from_geoarrow_chunks(parsed_chunks_refs.as_slice())?
                            .as_ref()
                            .downcast(true)
                    }
                };

                let new_field = new_geometry.data_type().to_field_with_metadata(
                    orig_field.name(),
                    orig_field.is_nullable(),
                    &geoarray_metadata,
                );
                let new_arrays = new_geometry.array_refs();

                let mut new_table = self.clone();
                new_table.set_column(index, new_field.into(), new_arrays)?;
                Ok(new_table)
            }
        }
    }

    /// Returns the length of this table.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "geozero")]
    /// # {
    /// use std::fs::File;
    ///
    /// let file = File::open("fixtures/roads.geojson").unwrap();
    /// let table = geoarrow::io::geojson::read_geojson(file, Default::default()).unwrap();
    /// assert_eq!(table.len(), 21);
    /// # }
    /// ```
    pub fn len(&self) -> usize {
        self.batches.iter().fold(0, |sum, val| sum + val.num_rows())
    }

    /// Returns true if this table is empty.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "geozero")]
    /// # {
    /// use std::fs::File;
    ///
    /// let file = File::open("fixtures/roads.geojson").unwrap();
    /// let table = geoarrow::io::geojson::read_geojson(file, Default::default()).unwrap();
    /// assert!(!table.is_empty());
    /// # }
    /// ```
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Consumes this table, returning its schema and its record batches.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "geozero")]
    /// # {
    /// use std::fs::File;
    ///
    /// let file = File::open("fixtures/roads.geojson").unwrap();
    /// let table = geoarrow::io::geojson::read_geojson(file, Default::default()).unwrap();
    /// let (batches, schema) = table.into_inner();
    /// # }
    /// ```
    pub fn into_inner(self) -> (Vec<RecordBatch>, SchemaRef) {
        (self.batches, self.schema)
    }

    /// Returns a reference to this table's schema.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "geozero")]
    /// # {
    /// use std::fs::File;
    ///
    /// let file = File::open("fixtures/roads.geojson").unwrap();
    /// let table = geoarrow::io::geojson::read_geojson(file, Default::default()).unwrap();
    /// let schema = table.schema();
    /// # }
    /// ```
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// Returns an immutable slice of this table's record batches.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "geozero")]
    /// # {
    /// use std::fs::File;
    ///
    /// let file = File::open("fixtures/roads.geojson").unwrap();
    /// let table = geoarrow::io::geojson::read_geojson(file, Default::default()).unwrap();
    /// let record_batches = table.batches();
    /// # }
    /// ```
    pub fn batches(&self) -> &[RecordBatch] {
        &self.batches
    }

    /// Returns this table's default geometry index.
    ///
    /// # Errors
    ///
    /// Returns an error if there is more than one geometry column.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "geozero")]
    /// # {
    /// use std::fs::File;
    ///
    /// let file = File::open("fixtures/roads.geojson").unwrap();
    /// let table = geoarrow::io::geojson::read_geojson(file, Default::default()).unwrap();
    /// assert_eq!(table.default_geometry_column_idx().unwrap(), 6);
    /// # }
    /// ```
    pub fn default_geometry_column_idx(&self) -> Result<usize> {
        let geom_col_indices = self.schema.as_ref().geometry_columns();
        if geom_col_indices.len() != 1 {
            Err(GeoArrowError::General(
                "Cannot use default geometry column when multiple geometry columns exist in table"
                    .to_string(),
            ))
        } else {
            Ok(geom_col_indices[0])
        }
    }

    /// Returns a reference to the chunked geometry array at the given index.
    ///
    /// If index is `None` and there is only one geometry column, that array
    /// will be returned. Otherwise, this method will return an error.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "geozero")]
    /// # {
    /// use std::fs::File;
    ///
    /// let file = File::open("fixtures/roads.geojson").unwrap();
    /// let table = geoarrow::io::geojson::read_geojson(file, Default::default()).unwrap();
    /// let chunked_array = table.geometry_column(None).unwrap(); // there's only one geometry column
    /// # }
    /// ```

    // TODO: make this generic across RecordBatch and Table
    // Deduplicate implementation in Python binding `geometry()`
    pub fn geometry_column(&self, index: Option<usize>) -> Result<Arc<dyn ChunkedNativeArray>> {
        let index = if let Some(index) = index {
            index
        } else {
            let geom_indices = self.schema.as_ref().geometry_columns();
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

    /// Returns a vector of references to all geometry chunked arrays.
    ///
    /// This may return an empty `Vec` if there are no geometry columns in the table, or may return
    /// more than one element if there are multiple geometry columns.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "geozero")]
    /// # {
    /// use std::fs::File;
    ///
    /// let file = File::open("fixtures/roads.geojson").unwrap();
    /// let table = geoarrow::io::geojson::read_geojson(file, Default::default()).unwrap();
    /// let chunked_arrays = table.geometry_columns().unwrap();
    /// assert_eq!(chunked_arrays.len(), 1);
    /// # }
    /// ```
    pub fn geometry_columns(&self) -> Result<Vec<Arc<dyn ChunkedNativeArray>>> {
        self.schema
            .as_ref()
            .geometry_columns()
            .into_iter()
            .map(|index| self.geometry_column(Some(index)))
            .collect()
    }

    /// Returns the number of columns in this table.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "geozero")]
    /// # {
    /// use std::fs::File;
    ///
    /// let file = File::open("fixtures/roads.geojson").unwrap();
    /// let table = geoarrow::io::geojson::read_geojson(file, Default::default()).unwrap();
    /// assert_eq!(table.num_columns(), 7);
    /// # }
    /// ```
    pub fn num_columns(&self) -> usize {
        self.schema.fields().len()
    }

    /// Replaces the column at index `i` with the given field and arrays.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "geozero")]
    /// # {
    /// use std::{sync::Arc, fs::File};
    /// use arrow_schema::{DataType, Field};
    /// use arrow_array::Int32Array;
    ///
    /// let file = File::open("fixtures/roads.geojson").unwrap();
    /// let mut table = geoarrow::io::geojson::read_geojson(file, Default::default()).unwrap();
    /// let indices: Vec<_> = (0..table.len()).map(|n| i32::try_from(n).unwrap()).collect();
    /// let array = Int32Array::from(indices);
    /// let field = Field::new("id", DataType::Int32, false);
    /// table.set_column(0, field.into(), vec![Arc::new(array)]).unwrap();
    /// # }
    /// ```
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

    /// Appends a column to this table, returning its new index.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "geozero")]
    /// # {
    /// use std::{sync::Arc, fs::File};
    /// use arrow_schema::{DataType, Field};
    /// use arrow_array::Int32Array;
    ///
    /// let file = File::open("fixtures/roads.geojson").unwrap();
    /// let mut table = geoarrow::io::geojson::read_geojson(file, Default::default()).unwrap();
    /// let indices: Vec<_> = (0..table.len()).map(|n| i32::try_from(n).unwrap()).collect();
    /// let array = Int32Array::from(indices);
    /// let field = Field::new("id", DataType::Int32, false);
    /// let index = table.append_column(field.into(), vec![Arc::new(array)]).unwrap();
    /// assert_eq!(index, 7);
    /// # }
    /// ```
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

    /// Convert this table into a [RecordBatchIterator]
    pub fn into_record_batch_reader(self) -> Box<dyn RecordBatchReader + Send> {
        Box::new(RecordBatchIterator::new(
            self.batches.into_iter().map(Ok),
            self.schema,
        ))
    }

    /// Convert this table into a [RecordBatchIterator], cloning each internal [RecordBatch]
    pub fn to_record_batch_reader<'a>(&'a self) -> Box<dyn RecordBatchReader + Send + 'a> {
        Box::new(RecordBatchIterator::new(
            self.batches.iter().map(|batch| Ok(batch.clone())),
            self.schema.clone(),
        ))
    }
}

impl TryFrom<Box<dyn arrow_array::RecordBatchReader>> for Table {
    type Error = GeoArrowError;

    fn try_from(
        value: Box<dyn arrow_array::RecordBatchReader>,
    ) -> std::result::Result<Self, Self::Error> {
        let schema = value.schema();
        let batches = value
            .into_iter()
            .collect::<std::result::Result<Vec<_>, ArrowError>>()?;
        Table::try_new(batches, schema)
    }
}

impl TryFrom<Box<dyn arrow_array::RecordBatchReader + Send>> for Table {
    type Error = GeoArrowError;

    fn try_from(
        value: Box<dyn arrow_array::RecordBatchReader + Send>,
    ) -> std::result::Result<Self, Self::Error> {
        let schema = value.schema();
        let batches = value
            .into_iter()
            .collect::<std::result::Result<Vec<_>, ArrowError>>()?;
        Table::try_new(batches, schema)
    }
}
