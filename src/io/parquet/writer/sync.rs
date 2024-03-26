use std::io::Write;

use crate::error::Result;
use crate::io::parquet::writer::encode::encode_record_batch;
use crate::io::parquet::writer::metadata::GeoParquetMetadataBuilder;
use crate::io::parquet::writer::options::GeoParquetWriterOptions;
use crate::table::GeoTable;
use parquet::arrow::ArrowWriter;
use parquet::file::metadata::KeyValue;

pub fn write_geoparquet<W: Write + Send>(
    table: &mut GeoTable,
    writer: W,
    options: &GeoParquetWriterOptions,
) -> Result<()> {
    // TODO: really we want the _metadata builder_ to be mutable but the output schema to be
    // immutable after here.
    let mut metadata_builder = GeoParquetMetadataBuilder::try_new(table, options)?;

    let mut writer = ArrowWriter::try_new(
        writer,
        metadata_builder.output_schema.clone(),
        options.writer_properties.clone(),
    )?;

    for input_batch in table.batches() {
        let encoded_batch = encode_record_batch(input_batch, &mut metadata_builder)?;
        writer.write(&encoded_batch)?;
    }

    let geo_meta = metadata_builder.finish();
    let kv_metadata = KeyValue::new("geo".to_string(), serde_json::to_string(&geo_meta)?);

    writer.append_key_value_metadata(kv_metadata);

    writer.close()?;

    Ok(())
}
