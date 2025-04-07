use std::io::Write;

use geoarrow_array::error::Result;
use crate::writer::encode::encode_record_batch;
use crate::writer::metadata::GeoParquetMetadataBuilder;
use crate::writer::options::GeoParquetWriterOptions;
use arrow_array::{RecordBatch, RecordBatchReader};
use arrow_schema::Schema;
use parquet::arrow::ArrowWriter;
use parquet::file::metadata::KeyValue;

/// Write a [RecordBatchReader] to GeoParquet.
pub fn write_geoparquet<W: Write + Send>(
    stream: Box<dyn RecordBatchReader>,
    writer: W,
    options: &GeoParquetWriterOptions,
) -> Result<()> {
    let mut parquet_writer = GeoParquetWriter::try_new(writer, &stream.schema(), options)?;

    for batch in stream {
        parquet_writer.write_batch(&batch?)?;
    }

    parquet_writer.finish()?;
    Ok(())
}

/// A synchronous GeoParquet file writer
pub struct GeoParquetWriter<W: Write + Send> {
    writer: ArrowWriter<W>,
    metadata_builder: GeoParquetMetadataBuilder,
}

impl<W: Write + Send> GeoParquetWriter<W> {
    /// Construct a new [GeoParquetWriter]
    pub fn try_new(writer: W, schema: &Schema, options: &GeoParquetWriterOptions) -> Result<Self> {
        let metadata_builder = GeoParquetMetadataBuilder::try_new(schema, options)?;

        let writer = ArrowWriter::try_new(
            writer,
            metadata_builder.output_schema.clone(),
            options.writer_properties.clone(),
        )?;

        Ok(Self {
            writer,
            metadata_builder,
        })
    }

    /// Write a batch to an output file
    pub fn write_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        let encoded_batch = encode_record_batch(batch, &mut self.metadata_builder)?;
        self.writer.write(&encoded_batch)?;
        Ok(())
    }

    /// Access the underlying writer.
    pub fn writer(&self) -> &ArrowWriter<W> {
        &self.writer
    }

    /// Close and finalize the writer.
    ///
    /// This must be called to write the Parquet footer.
    ///
    /// All the data in the inner buffer will be force flushed.
    pub fn finish(mut self) -> Result<()> {
        if let Some(geo_meta) = self.metadata_builder.finish() {
            let kv_metadata = KeyValue::new("geo".to_string(), serde_json::to_string(&geo_meta)?);
            self.writer.append_key_value_metadata(kv_metadata);
        }

        self.writer.close()?;
        Ok(())
    }
}
