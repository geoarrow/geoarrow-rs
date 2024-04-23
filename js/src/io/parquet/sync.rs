use arrow_wasm::Table;
// use parquet_wasm::utils::assert_parquet_file_not_empty;
use bytes::Bytes;
use geoarrow::io::parquet::{
    read_geoparquet as _read_geoparquet, write_geoparquet as _write_geoparquet,
    ParquetReaderOptions,
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
    let options = ParquetReaderOptions {
        batch_size: Some(65536),
        ..Default::default()
    };
    let geo_table = _read_geoparquet(Bytes::from(file), options)?;
    let (schema, batches) = geo_table.into_inner();
    Ok(Table::new(schema, batches))
}

/// Write table to GeoParquet
///
/// Note that this consumes the table input
#[wasm_bindgen(js_name = writeGeoParquet)]
pub fn write_geoparquet(table: Table) -> WasmResult<Vec<u8>> {
    let (schema, batches) = table.into_inner();
    let mut rust_table = geoarrow::table::Table::try_new(schema, batches)?;
    let mut output_file: Vec<u8> = vec![];
    _write_geoparquet(&mut rust_table, &mut output_file, &Default::default())?;
    Ok(output_file)
}
