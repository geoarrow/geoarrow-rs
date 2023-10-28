use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub struct BooleanArray(pub(crate) arrow_array::BooleanArray);

#[wasm_bindgen]
impl BooleanArray {
    // TODO:
    // #[wasm_bindgen]
    // pub fn new() {
    //     arrow2::array::BooleanArray::f
    // }
}

#[wasm_bindgen]
pub struct FloatArray(pub(crate) arrow_array::Float64Array);

#[wasm_bindgen]
impl FloatArray {
    #[wasm_bindgen(constructor)]
    pub fn new(values: Vec<f64>) -> Self {
        Self(values.into())
    }
}
