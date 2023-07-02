use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub struct LineStringArray(pub(crate) geoarrow::array::LineStringArray);
