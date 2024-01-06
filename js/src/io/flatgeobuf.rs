use std::io::Cursor;

use geoarrow::io::flatgeobuf::read_flatgeobuf as _read_flatgeobuf;
// use parquet_wasm::utils::assert_parquet_file_not_empty;
use wasm_bindgen::prelude::*;

use crate::error::WasmResult;
use crate::table::GeoTable;

/// Read a FlatGeobuf file into GeoArrow memory
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
#[wasm_bindgen(js_name = readFlatGeobuf)]
pub fn read_flatgeobuf(file: &[u8], batch_size: Option<usize>) -> WasmResult<GeoTable> {
    // assert_parquet_file_not_empty(parquet_file)?;
    let mut cursor = Cursor::new(file);
    let geo_table = _read_flatgeobuf(&mut cursor, Default::default(), batch_size)?;
    Ok(GeoTable(geo_table))
}
