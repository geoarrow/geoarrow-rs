use std::io::Cursor;

use geoarrow::io::geojson::{read_geojson as _read_geojson, write_geojson as _write_geojson};
// use parquet_wasm::utils::assert_parquet_file_not_empty;
use wasm_bindgen::prelude::*;

use crate::error::WasmResult;
use arrow_wasm::Table;

/// Read a GeoJSON file into GeoArrow memory
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
/// @param file Uint8Array containing FlatGeobuf data
/// @returns Uint8Array containing Arrow data in [IPC Stream format](https://arrow.apache.org/docs/format/Columnar.html#ipc-streaming-format). To parse this into an Arrow table, pass to `tableFromIPC` in the Arrow JS bindings.
#[wasm_bindgen(js_name = readGeoJSON)]
pub fn read_geojson(file: &[u8], batch_size: Option<usize>) -> WasmResult<Table> {
    // assert_parquet_file_not_empty(parquet_file)?;
    let mut cursor = Cursor::new(file);
    let geo_table = _read_geojson(&mut cursor, batch_size)?;
    let (schema, batches) = geo_table.into_inner();
    Ok(Table::new(schema, batches))
}

/// Write table to GeoJSON
///
/// Note that this consumes the table input
#[wasm_bindgen(js_name = writeGeoJSON)]
pub fn write_geojson(table: Table) -> WasmResult<Vec<u8>> {
    let (schema, batches) = table.into_inner();
    let rust_table = geoarrow::table::Table::try_new(schema, batches)?;
    let mut output_file: Vec<u8> = vec![];
    _write_geojson(rust_table, &mut output_file)?;
    Ok(output_file)
}
