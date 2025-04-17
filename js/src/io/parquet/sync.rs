use arrow_array::{RecordBatchIterator, RecordBatchReader};
use arrow_wasm::Table;
// use parquet_wasm::utils::assert_parquet_file_not_empty;
use bytes::Bytes;
use geoarrow_geoparquet::{
    GeoParquetReaderOptions, GeoParquetRecordBatchReaderBuilder,
    write_geoparquet as _write_geoparquet,
};
use wasm_bindgen::prelude::*;

use crate::error::WasmResult;

/// Read a GeoParquet file into GeoArrow memory
///
/// Example:
///
/// ```js
/// import { tableFromIPC } from "apache-arrow";
/// // Edit the `parquet-wasm` import as necessary
/// import { readParquet } from "parquet-wasm/node2";
///
/// const resp = await fetch("https://example.com/file.parquet");
/// const parquetUint8Array = new Uint8Array(await resp.arrayBuffer());
/// const arrowUint8Array = readParquet(parquetUint8Array);
/// const arrowTable = tableFromIPC(arrowUint8Array);
/// ```
///
/// @param file Uint8Array containing GeoParquet data
/// @returns Uint8Array containing Arrow data in [IPC Stream format](https://arrow.apache.org/docs/format/Columnar.html#ipc-streaming-format). To parse this into an Arrow table, pass to `tableFromIPC` in the Arrow JS bindings.
#[wasm_bindgen(js_name = readGeoParquet)]
pub fn read_geoparquet(file: Vec<u8>) -> WasmResult<Table> {
    // assert_parquet_file_not_empty(parquet_file)?;
    let geo_options = GeoParquetReaderOptions::default().with_batch_size(65536);
    let reader = GeoParquetRecordBatchReaderBuilder::try_new_with_options(
        Bytes::from(file),
        Default::default(),
        geo_options,
    )?
    .build()?;
    let schema = reader.schema();
    let batches = reader.collect::<Result<Vec<_>, _>>()?;
    Ok(Table::new(schema, batches))
}

/// Write table to GeoParquet
///
/// Note that this consumes the table input
#[wasm_bindgen(js_name = writeGeoParquet)]
pub fn write_geoparquet(table: Table) -> WasmResult<Vec<u8>> {
    let (schema, batches) = table.into_inner();
    let record_batch_reader = Box::new(RecordBatchIterator::new(
        batches.into_iter().map(Ok),
        schema,
    ));
    let mut output_file: Vec<u8> = vec![];
    _write_geoparquet(record_batch_reader, &mut output_file, &Default::default())?;
    Ok(output_file)
}
