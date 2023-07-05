use crate::error::{GeoArrowError, Result};
use arrow2::array::Array;
use arrow2::datatypes::Field;
use arrow2::ffi;
use wasm_bindgen::prelude::*;

/// Wrapper around an ArrowArray FFI schema and array struct in Wasm memory.
#[wasm_bindgen]
pub struct FFIArrowArray(Box<ffi::ArrowSchema>, Box<ffi::ArrowArray>);

impl FFIArrowArray {
    pub fn new(field: &Field, array: Box<dyn Array>) -> Self {
        let field = Box::new(ffi::export_field_to_c(field));
        let array = Box::new(ffi::export_array_to_c(array));
        Self(field, array)
    }
}

impl From<(&Field, Box<dyn Array>)> for FFIArrowArray {
    fn from(value: (&Field, Box<dyn Array>)) -> Self {
        Self::new(value.0, value.1)
    }
}

impl TryFrom<FFIArrowArray> for Box<dyn Array> {
    type Error = GeoArrowError;

    fn try_from(ffi_array: FFIArrowArray) -> Result<Self> {
        let field = unsafe { ffi::import_field_from_c(&ffi_array.0) }?;
        let array = unsafe { ffi::import_array_from_c(*ffi_array.1, field.data_type) }?;
        Ok(array)
    }
}

#[wasm_bindgen]
impl FFIArrowArray {
    #[wasm_bindgen]
    pub fn field_addr(&self) -> *const ffi::ArrowSchema {
        self.0.as_ref() as *const _
    }

    #[wasm_bindgen]
    pub fn array_addr(&self) -> *const ffi::ArrowArray {
        self.1.as_ref() as *const _
    }

    #[wasm_bindgen]
    pub fn free(self) {
        drop(self.0)
    }

    #[wasm_bindgen]
    pub fn drop(self) {
        drop(self.0)
    }
}
