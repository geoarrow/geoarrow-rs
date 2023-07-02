use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub struct MultiLineStringArray(pub(crate) geoarrow::array::MultiLineStringArray);
