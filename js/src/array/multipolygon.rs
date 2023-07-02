use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub struct MultiPolygonArray(pub(crate) geoarrow::array::MultiPolygonArray);
