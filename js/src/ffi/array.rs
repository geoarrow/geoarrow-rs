use crate::error::{GeoArrowError, Result};
use arrow2::array::Array;
use arrow2::datatypes::Field;
use arrow2::ffi;
use wasm_bindgen::prelude::*;

/// A pointer to an Arrow array in WebAssembly memory.
///
/// Using [`arrow-js-ffi`](https://github.com/kylebarron/arrow-js-ffi), you can view or copy Arrow these objects to JavaScript.
///
/// ```ts
/// import { parseField, parseVector } from "arrow-js-ffi";
///
/// // You need to access the geoarrow webassembly memory space.
/// // The way to do this is different per geoarrow bundle method.
/// const WASM_MEMORY: WebAssembly.Memory = geoarrow.__wasm.memory;
///
/// // Say we have a point array from somewhere
/// const pointArray: geoarrow.PointArray = ...;
///
/// // Export this existing point array to wasm.
/// const ffiArray = pointArray.toFfi();
///
/// // Parse an arrow-js field object from the pointer
/// const jsArrowField = parseField(WASM_MEMORY.buffer, ffiArray.field_addr());
///
/// // Parse an arrow-js vector from the pointer and parsed field
/// const jsPointVector = parseVector(
///   WASM_MEMORY.buffer,
///   ffiArray.array_addr(),
///   field.type
/// );
/// ```
///
/// ## Memory management
///
/// Note that this array will not be released automatically. You need to manually call `.free()` to
/// release memory.
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
