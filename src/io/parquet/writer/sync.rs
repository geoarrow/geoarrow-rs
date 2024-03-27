use std::io::Write;

use crate::error::Result;
use crate::io::parquet::writer::encode::encode_record_batch;
use crate::io::parquet::writer::metadata::GeoParquetMetadataBuilder;
use crate::io::parquet::writer::options::GeoParquetWriterOptions;
use crate::table::GeoTable;
use arrow_array::RecordBatch;
use arrow_schema::Schema;
use parquet::arrow::ArrowWriter;
use parquet::file::metadata::KeyValue;

pub fn write_geoparquet<W: Write + Send>(
    table: &mut GeoTable,
    writer: W,
    options: &GeoParquetWriterOptions,
) -> Result<()> {
    let mut parquet_writer = GeoParquetWriter::try_new(writer, table.schema(), options)?;

    for batch in table.batches() {
        parquet_writer.write_batch(batch)?;
    }

    parquet_writer.finish()?;
    Ok(())
}

pub struct GeoParquetWriter<W: Write + Send> {
    writer: ArrowWriter<W>,
    metadata_builder: GeoParquetMetadataBuilder,
}

impl<W: Write + Send> GeoParquetWriter<W> {
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

    pub fn write_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        let encoded_batch = encode_record_batch(batch, &mut self.metadata_builder)?;
        self.writer.write(&encoded_batch)?;
        Ok(())
    }

    pub fn finish(mut self) -> Result<()> {
        if let Some(geo_meta) = self.metadata_builder.finish() {
            let kv_metadata = KeyValue::new("geo".to_string(), serde_json::to_string(&geo_meta)?);
            self.writer.append_key_value_metadata(kv_metadata);
        }

        self.writer.close()?;
        Ok(())
    }
}
