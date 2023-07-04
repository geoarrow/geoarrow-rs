use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub struct BooleanArray(pub(crate) arrow2::array::BooleanArray);

#[wasm_bindgen]
pub struct Float64Array(pub(crate) arrow2::array::PrimitiveArray<f64>);
