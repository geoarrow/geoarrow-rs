use std::io::Cursor;

use geoarrow::io::flatgeobuf::read_flatgeobuf as _read_flatgeobuf;
use wasm_bindgen::prelude::*;

use crate::error::WasmResult;
use crate::table::GeoTable;

/// Read a FlatGeobuf file into GeoArrow memory
///
/// @param file Uint8Array containing FlatGeobuf data
/// @returns GeoArrow table
#[wasm_bindgen(js_name = readFlatGeobuf)]
pub fn read_flatgeobuf(file: &[u8], batch_size: Option<usize>) -> WasmResult<GeoTable> {
    let mut cursor = Cursor::new(file);
    let geo_table = _read_flatgeobuf(&mut cursor, Default::default(), batch_size)?;
    Ok(GeoTable(geo_table))
}
