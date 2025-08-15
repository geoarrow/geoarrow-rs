use arrow_array::{RecordBatch, RecordBatchReader};
use arrow_schema::{ArrowError, SchemaRef};
use geoarrow_schema::error::GeoArrowResult;
use parquet::arrow::arrow_reader::ParquetRecordBatchReader;

use crate::reader::parse::{parse_record_batch, validate_target_schema};

/// A wrapper around a [`ParquetRecordBatchReader`] to apply GeoArrow metadata onto emitted
/// [`RecordBatch`]es.
///
/// This implements [`RecordBatchReader`], which means it also implements `Iterator<Item =
/// ArrowResult<RecordBatch>>`.
pub struct GeoParquetRecordBatchReader {
    reader: ParquetRecordBatchReader,
    target_schema: SchemaRef,
}

impl GeoParquetRecordBatchReader {
    /// Create a new [`GeoParquetRecordBatchReader`] from a [`ParquetRecordBatchReader`].
    ///
    /// Use [`geoarrow_schema`][crate::reader::GeoParquetReaderBuilder::geoarrow_schema] to infer a
    /// GeoArrow schema.
    ///
    /// This will validate that the target schema is compatible with the original schema.
    pub fn try_new(
        reader: ParquetRecordBatchReader,
        target_schema: SchemaRef,
    ) -> GeoArrowResult<Self> {
        validate_target_schema(&reader.schema(), &target_schema)?;
        Ok(Self {
            reader,
            target_schema,
        })
    }
}

impl Iterator for GeoParquetRecordBatchReader {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(batch) = self.reader.next() {
            match batch {
                Ok(batch) => Some(
                    parse_record_batch(batch, self.target_schema.clone()).map_err(|err| err.into()),
                ),
                Err(err) => Some(Err(err)),
            }
        } else {
            None
        }
    }
}

impl RecordBatchReader for GeoParquetRecordBatchReader {
    fn schema(&self) -> SchemaRef {
        self.target_schema.clone()
    }
}
