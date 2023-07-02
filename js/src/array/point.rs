use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub struct PointArray(pub(crate) geoarrow::array::PointArray);
