use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub struct MultiPointArray(pub(crate) geoarrow::array::MultiPointArray);
