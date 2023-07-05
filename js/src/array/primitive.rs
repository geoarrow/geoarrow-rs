use crate::array::ffi::FFIArrowArray;
use arrow2::datatypes::Field;
use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub struct BooleanArray(pub(crate) arrow2::array::BooleanArray);

#[wasm_bindgen]
impl BooleanArray {
    #[wasm_bindgen]
    pub fn to_ffi(&self) -> FFIArrowArray {
        let arrow_array = self.0.clone().boxed();
        let field = Field::new("", arrow_array.data_type().clone(), true);
        FFIArrowArray::new(&field, arrow_array)
    }
}

#[wasm_bindgen]
pub struct Float64Array(pub(crate) arrow2::array::PrimitiveArray<f64>);

#[wasm_bindgen]
impl Float64Array {
    #[wasm_bindgen]
    pub fn to_ffi(&self) -> FFIArrowArray {
        let arrow_array = self.0.clone().boxed();
        let field = Field::new("", arrow_array.data_type().clone(), true);
        FFIArrowArray::new(&field, arrow_array)
    }
}
