use arrow_wasm::Table;
use bytes::Bytes;
use geoarrow_schema::CoordType;
use geoparquet::metadata::GeoParquetMetadata;
use geoparquet::reader::{GeoParquetReaderBuilder, GeoParquetRecordBatchReader};
use geoparquet::writer::{
    GeoParquetRecordBatchEncoder, GeoParquetWriterEncoding, GeoParquetWriterOptions,
    GeoParquetWriterOptionsBuilder,
};
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde::Deserialize;
use wasm_bindgen::prelude::*;

use crate::error::WasmResult;

#[wasm_bindgen(typescript_custom_section)]
const TS_GEOPARQUET_WRITER_OPTIONS: &'static str = r#"
/** Options for writeGeoParquet. Defaults match the Rust crate's defaults. */
export interface GeoParquetWriterOptions {
  /**
   * Geometry encoding: "wkb" (default; GeoParquet 1.0-compatible) or
   * "native" (GeoArrow-native encoding, GeoParquet 1.1).
   */
  encoding?: "wkb" | "native";
  /**
   * Write a per-row bbox covering column and covering metadata
   * (default false). Required for the written file to be readable with
   * `ParquetFile.read(bbox)` when the encoding is "wkb".
   */
  generateCovering?: boolean;
}
"#;

#[wasm_bindgen]
extern "C" {
    #[wasm_bindgen(typescript_type = "GeoParquetWriterOptions")]
    pub type JsGeoParquetWriterOptions;
}

// serde-wasm-bindgen reads the declared fields only, thus an unknown key passes
// without notice and the TypeScript interface is what catches a typo. An unknown
// value for a known key still errors below.
#[derive(Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct WriterOptions {
    encoding: Option<String>,
    generate_covering: Option<bool>,
}

impl WriterOptions {
    fn into_geoparquet_options(self) -> WasmResult<GeoParquetWriterOptions> {
        let mut builder = GeoParquetWriterOptionsBuilder::default();
        if let Some(encoding) = self.encoding.as_deref() {
            let encoding = match encoding {
                "wkb" => GeoParquetWriterEncoding::WKB,
                "native" => GeoParquetWriterEncoding::GeoArrow,
                other => {
                    return Err(JsError::new(&format!(
                        "writeGeoParquet: unknown encoding '{other}' (expected \"wkb\" or \"native\")"
                    )));
                }
            };
            builder = builder.set_encoding(encoding);
        }
        if let Some(generate_covering) = self.generate_covering {
            builder = builder.set_generate_covering(generate_covering);
        }
        Ok(builder.build())
    }
}

/// Read a GeoParquet file into an Arrow Table, with each geometry column in its
/// native GeoArrow type
#[wasm_bindgen(js_name = readGeoParquet)]
pub fn read_geoparquet(file: Vec<u8>) -> WasmResult<Table> {
    let builder =
        ParquetRecordBatchReaderBuilder::try_new(Bytes::from(file))?.with_batch_size(65536);
    let geo_metadata: GeoParquetMetadata = builder
        .geoparquet_metadata()
        .ok_or_else(|| JsError::new("readGeoParquet: file lacks GeoParquet metadata"))??;
    // Parse a WKB geometry column into a native GeoArrow array, thus a kernel
    // downstream sees a typed geometry.
    let parse_to_native = true;
    let target_schema =
        builder.geoarrow_schema(&geo_metadata, parse_to_native, CoordType::Interleaved)?;
    let parquet_reader = builder.build()?;
    let geo_reader = GeoParquetRecordBatchReader::try_new(parquet_reader, target_schema.clone())?;
    let batches: Vec<_> = geo_reader.collect::<Result<Vec<_>, _>>()?;
    Ok(Table::new(target_schema, batches))
}

/// Encode an Arrow Table as GeoParquet bytes
///
/// Note that this consumes the table input, that each geometry column must
/// carry GeoArrow extension metadata for the encoder to find it, and that the
/// default of no bbox covering gives a file that `ParquetFile.read(bbox)`
/// refuses. Pass `{ generateCovering: true }` to read the output by bbox.
#[wasm_bindgen(js_name = writeGeoParquet)]
pub fn write_geoparquet(
    table: Table,
    options: Option<JsGeoParquetWriterOptions>,
) -> WasmResult<Vec<u8>> {
    let options = match options {
        Some(js_options) => {
            let parsed: WriterOptions = serde_wasm_bindgen::from_value(js_options.obj)
                .map_err(|e| JsError::new(&format!("writeGeoParquet: invalid options: {e}")))?;
            parsed.into_geoparquet_options()?
        }
        None => GeoParquetWriterOptions::default(),
    };
    let (schema, batches) = table.into_inner();
    let mut encoder = GeoParquetRecordBatchEncoder::try_new(&schema, &options)?;
    let target_schema = encoder.target_schema();
    let mut buffer: Vec<u8> = Vec::new();
    {
        let mut writer = ArrowWriter::try_new(&mut buffer, target_schema, None)?;
        for batch in &batches {
            let encoded = encoder.encode_record_batch(batch)?;
            writer.write(&encoded)?;
        }
        let kv = encoder.into_keyvalue()?;
        writer.append_key_value_metadata(kv);
        writer.close()?;
    }
    Ok(buffer)
}
