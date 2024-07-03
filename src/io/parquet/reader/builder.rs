use arrow_array::{RecordBatch, RecordBatchReader};
use arrow_schema::{ArrowError, Schema, SchemaRef};
use parquet::arrow::arrow_reader::{
    ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReader,
    ParquetRecordBatchReaderBuilder, RowSelection,
};
use parquet::arrow::ProjectionMask;
use parquet::file::reader::ChunkReader;

use crate::array::CoordType;
use crate::error::Result;
use crate::io::parquet::metadata::GeoParquetMetadata;
use crate::io::parquet::reader::options::GeoParquetReaderOptions;
use crate::io::parquet::ParquetBboxPaths;
use crate::table::Table;

pub struct GeoParquetReaderBuilder2 {}

pub trait GeoParquetReaderBuilder: Sized {
    fn output_schema(&self) -> SchemaRef;
}

pub struct SyncGeoParquetReaderBuilder<T: ChunkReader + 'static> {
    builder: ParquetRecordBatchReaderBuilder<T>,
    geo_meta: Option<GeoParquetMetadata>,
    options: GeoParquetReaderOptions,
}

impl<T: ChunkReader + 'static> SyncGeoParquetReaderBuilder<T> {
    pub fn try_new(reader: T) -> Result<Self> {
        Self::try_new_with_options(reader, Default::default())
    }

    pub fn try_new_with_options(reader: T, options: ArrowReaderOptions) -> Result<Self> {
        let metadata = ArrowReaderMetadata::load(&reader, options)?;
        Ok(Self::new_with_metadata(reader, metadata))
    }

    pub fn new_with_metadata(input: T, metadata: ArrowReaderMetadata) -> Self {
        let builder = ParquetRecordBatchReaderBuilder::new_with_metadata(input, metadata);
        Self::from_builder(builder)
    }

    pub fn from_builder(builder: ParquetRecordBatchReaderBuilder<T>) -> Self {
        let geo_meta =
            GeoParquetMetadata::from_parquet_meta(builder.metadata().file_metadata()).ok();
        Self { builder, geo_meta }
    }

    pub fn build(self) -> Result<GeoParquetRecordBatchReader> {
        let output_schema = self.output_schema();
        let reader = self.builder.build()?;
        Ok(GeoParquetRecordBatchReader {
            reader,
            output_schema,
        })
    }
}

impl<T: ChunkReader + 'static> GeoParquetReaderBuilder for SyncGeoParquetReaderBuilder<T> {
    fn output_schema(&self) -> SchemaRef {
        todo!()
    }
}

/// An `Iterator<Item = ArrowResult<RecordBatch>>` that yields [`RecordBatch`]
/// read from a Parquet data source.
/// This will parse any geometries to their native representation.
pub struct GeoParquetRecordBatchReader {
    reader: ParquetRecordBatchReader,
    output_schema: SchemaRef,
}

impl GeoParquetRecordBatchReader {
    /// Read
    pub fn read_table(self) -> Result<Table> {
        let output_schema = self.output_schema.clone();
        let batches = self.collect::<std::result::Result<Vec<_>, ArrowError>>()?;
        Table::try_new(output_schema, batches)
    }
}

impl Iterator for GeoParquetRecordBatchReader {
    type Item = std::result::Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(batch) = self.reader.next() {
            match batch {
                Ok(batch) => Some(parse_batch(batch, &self.output_schema)),
                Err(err) => Some(Err(err)),
            }
        } else {
            None
        }
    }
}

impl RecordBatchReader for GeoParquetRecordBatchReader {
    fn schema(&self) -> arrow_schema::SchemaRef {
        self.output_schema.clone()
    }
}

/// Parse an Arrow batch to an output schema
pub(crate) fn parse_batch(
    batch: RecordBatch,
    output_schema: &Schema,
) -> std::result::Result<RecordBatch, ArrowError> {
    Ok(batch)
}
