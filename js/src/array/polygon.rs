use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub struct PolygonArray(pub(crate) geoarrow::array::PolygonArray);
