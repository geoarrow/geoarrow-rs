use crate::array::*;
use crate::error::WasmResult;
use wasm_bindgen::prelude::*;

macro_rules! impl_vincenty_length {
    ($struct_name:ident) => {
        #[wasm_bindgen]
        impl $struct_name {
            #[wasm_bindgen]
            pub fn vincenty_length(&self) -> WasmResult<FloatArray> {
                use geoarrow::algorithm::geo::VincentyLength;
                Ok(FloatArray(VincentyLength::vincenty_length(&self.0)?))
            }
        }
    };
}

impl_vincenty_length!(PointArray);
impl_vincenty_length!(MultiPointArray);
impl_vincenty_length!(LineStringArray);
impl_vincenty_length!(MultiLineStringArray);
