use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub struct GeoTable(pub(crate) geoarrow::table::GeoTable);
