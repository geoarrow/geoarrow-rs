use crate::array::*;
use crate::ffi::FFIArrowArray;
use arrow2::datatypes::Field;
use geoarrow::GeometryArrayTrait;
use wasm_bindgen::prelude::*;

macro_rules! impl_to_ffi {
    ($struct_name:ident) => {
        #[wasm_bindgen]
        impl $struct_name {
            #[wasm_bindgen(js_name = toFfi)]
            pub fn to_ffi(&self) -> FFIArrowArray {
                let arrow_array = self.0.clone().into_boxed_arrow();
                let field = Field::new("", arrow_array.data_type().clone(), true);
                FFIArrowArray::new(&field, arrow_array)
            }
        }
    };
}

impl_to_ffi!(PointArray);
impl_to_ffi!(LineStringArray);
impl_to_ffi!(PolygonArray);
impl_to_ffi!(MultiPointArray);
impl_to_ffi!(MultiLineStringArray);
impl_to_ffi!(MultiPolygonArray);
impl_to_ffi!(GeometryArray);

macro_rules! impl_to_ffi_arrow2 {
    ($struct_name:ident) => {
        #[wasm_bindgen]
        impl $struct_name {
            #[wasm_bindgen(js_name = toFfi)]
            pub fn to_ffi(&self) -> FFIArrowArray {
                let arrow_array = self.0.clone().boxed();
                let field = Field::new("", arrow_array.data_type().clone(), true);
                FFIArrowArray::new(&field, arrow_array)
            }
        }
    };
}

impl_to_ffi_arrow2!(BooleanArray);
impl_to_ffi_arrow2!(FloatArray);
