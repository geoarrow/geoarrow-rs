use crate::error::WasmResult;
use crate::vector::*;
use arrow_wasm::arrow1::ffi::{FFIArrowSchema, FFIVector};
use geoarrow::GeometryArrayTrait;
use wasm_bindgen::prelude::*;

macro_rules! impl_vector {
    ($struct_name:ident) => {
        #[wasm_bindgen]
        impl $struct_name {
            /// Export this Vector to FFI structs according to the Arrow C Data Interface.
            ///
            /// This method **does not consume** the Vector, so you must remember to call `free` to
            /// release the resources. The underlying arrays are reference counted, so this method
            /// does not copy data, it only prevents the data from being released.
            #[wasm_bindgen(js_name = toFFI)]
            pub fn to_ffi(&self) -> WasmResult<FFIVector> {
                let field = self.0.extension_field();
                let ffi_schema: FFIArrowSchema = field.as_ref().try_into()?;
                let arrays = self
                    .0
                    .clone()
                    .into_inner()
                    .into_iter()
                    .map(|arr| arr.into_array_ref())
                    .collect::<Vec<_>>();
                Ok(FFIVector::from_arrow(arrays, Some(ffi_schema))?)
            }

            /// Export this Vector to FFI structs according to the Arrow C Data Interface.
            ///
            /// This method **does consume** the Vector, so the original Vector will be
            /// inaccessible after this call. You must still call {@linkcode FFIVector.free} after
            /// you've finished using the FFIVector.
            #[wasm_bindgen(js_name = intoFFI)]
            pub fn into_ffi(self) -> WasmResult<FFIVector> {
                let field = self.0.extension_field();
                let ffi_schema: FFIArrowSchema = field.as_ref().try_into()?;
                let arrays = self
                    .0
                    .into_inner()
                    .into_iter()
                    .map(|arr| arr.into_array_ref())
                    .collect::<Vec<_>>();
                Ok(FFIVector::from_arrow(arrays, Some(ffi_schema))?)
            }
        }
    };
}

impl_vector!(PointVector);
impl_vector!(LineStringVector);
impl_vector!(PolygonVector);
impl_vector!(MultiPointVector);
impl_vector!(MultiLineStringVector);
impl_vector!(MultiPolygonVector);
impl_vector!(MixedGeometryVector);
impl_vector!(GeometryCollectionVector);
impl_vector!(RectVector);
