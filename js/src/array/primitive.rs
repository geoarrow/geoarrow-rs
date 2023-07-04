use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub struct Float64Array(pub(crate) arrow2::array::PrimitiveArray<f64>);
