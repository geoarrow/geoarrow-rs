use geoarrow_array::error::Result;
use crate::writer::encode::encode_record_batch;
use crate::writer::metadata::GeoParquetMetadataBuilder;
use crate::writer::options::GeoParquetWriterOptions;
use arrow_array::{RecordBatch, RecordBatchReader};
use arrow_schema::Schema;
use parquet::arrow::async_writer::AsyncFileWriter;
use parquet::arrow::AsyncArrowWriter;
use parquet::file::metadata::KeyValue;

/// Write a [RecordBatchReader] to GeoParquet.
pub async fn write_geoparquet_async<W: AsyncFileWriter>(
    stream: Box<dyn RecordBatchReader>,
    writer: W,
    options: &GeoParquetWriterOptions,
) -> Result<()> {
    let mut parquet_writer = GeoParquetWriterAsync::try_new(writer, &stream.schema(), options)?;

    for batch in stream {
        parquet_writer.write_batch(&batch?).await?;
    }

    parquet_writer.finish().await?;
    Ok(())
}

/// An asynchronous GeoParquet file writer
pub struct GeoParquetWriterAsync<W: AsyncFileWriter> {
    writer: AsyncArrowWriter<W>,
    metadata_builder: GeoParquetMetadataBuilder,
}

impl<W: AsyncFileWriter> GeoParquetWriterAsync<W> {
    /// Construct a new [GeoParquetWriterAsync]
    pub fn try_new(writer: W, schema: &Schema, options: &GeoParquetWriterOptions) -> Result<Self> {
        let metadata_builder = GeoParquetMetadataBuilder::try_new(schema, options)?;

        let writer = AsyncArrowWriter::try_new(
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
    pub async fn write_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        let encoded_batch = encode_record_batch(batch, &mut self.metadata_builder)?;
        self.writer.write(&encoded_batch).await?;
        Ok(())
    }

    /// Access the underlying writer.
    pub fn writer(&self) -> &AsyncArrowWriter<W> {
        &self.writer
    }

    /// Close and finalize the writer.
    ///
    /// This must be called to write the Parquet footer.
    ///
    /// All the data in the inner buffer will be force flushed.
    pub async fn finish(mut self) -> Result<()> {
        if let Some(geo_meta) = self.metadata_builder.finish() {
            let kv_metadata = KeyValue::new("geo".to_string(), serde_json::to_string(&geo_meta)?);
            self.writer.append_key_value_metadata(kv_metadata);
        }

        self.writer.close().await?;
        Ok(())
    }
}
