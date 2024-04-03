use crate::error::Result;
use crate::io::parquet::writer::encode::encode_record_batch;
use crate::io::parquet::writer::metadata::GeoParquetMetadataBuilder;
use crate::io::parquet::writer::options::GeoParquetWriterOptions;
use crate::table::Table;
use arrow_array::RecordBatch;
use arrow_schema::Schema;
use parquet::arrow::AsyncArrowWriter;
use parquet::file::metadata::KeyValue;
use tokio::io::AsyncWrite;

pub async fn write_geoparquet_async<W: AsyncWrite + Unpin + Send>(
    table: &mut Table,
    writer: W,
    options: &GeoParquetWriterOptions,
) -> Result<()> {
    let mut parquet_writer = GeoParquetWriterAsync::try_new(writer, table.schema(), options)?;

    for batch in table.batches() {
        parquet_writer.write_batch(batch).await?;
    }

    parquet_writer.finish().await?;
    Ok(())
}

pub struct GeoParquetWriterAsync<W: AsyncWrite + Unpin + Send> {
    writer: AsyncArrowWriter<W>,
    metadata_builder: GeoParquetMetadataBuilder,
}

impl<W: AsyncWrite + Unpin + Send> GeoParquetWriterAsync<W> {
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

    pub async fn write_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        let encoded_batch = encode_record_batch(batch, &mut self.metadata_builder)?;
        self.writer.write(&encoded_batch).await?;
        Ok(())
    }

    pub fn writer(&self) -> &AsyncArrowWriter<W> {
        &self.writer
    }

    pub async fn finish(mut self) -> Result<()> {
        if let Some(geo_meta) = self.metadata_builder.finish() {
            let kv_metadata = KeyValue::new("geo".to_string(), serde_json::to_string(&geo_meta)?);
            self.writer.append_key_value_metadata(kv_metadata);
        }

        self.writer.close().await?;
        Ok(())
    }
}
