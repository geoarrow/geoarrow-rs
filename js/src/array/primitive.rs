use crate::array::ffi::FFIArrowArray;
use arrow2::datatypes::Field;
use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub struct BooleanArray(pub(crate) arrow2::array::BooleanArray);

#[wasm_bindgen]
impl BooleanArray {
    // TODO:
    // #[wasm_bindgen]
    // pub fn new() {
    //     arrow2::array::BooleanArray::f
    // }

    #[wasm_bindgen]
    pub fn to_ffi(&self) -> FFIArrowArray {
        let arrow_array = self.0.clone().boxed();
        let field = Field::new("", arrow_array.data_type().clone(), true);
        FFIArrowArray::new(&field, arrow_array)
    }
}

#[wasm_bindgen]
pub struct FloatArray(pub(crate) arrow2::array::PrimitiveArray<f64>);

#[wasm_bindgen]
impl FloatArray {
    #[wasm_bindgen(constructor)]
    pub fn new(values: Vec<f64>) -> Self {
        Self(arrow2::array::PrimitiveArray::from_vec(values))
    }

    #[wasm_bindgen]
    pub fn to_ffi(&self) -> FFIArrowArray {
        let arrow_array = self.0.clone().boxed();
        let field = Field::new("", arrow_array.data_type().clone(), true);
        FFIArrowArray::new(&field, arrow_array)
    }
}
