use crate::data::*;
use crate::error::WasmResult;
use crate::vector::*;
use arrow_wasm::data::Data;
use arrow_wasm::vector::Vector;
use geoarrow::algorithm::geo::EuclideanLength;
use wasm_bindgen::prelude::*;

macro_rules! impl_euclidean_length {
    ($struct_name:ident) => {
        #[wasm_bindgen]
        impl $struct_name {
            /// Calculation of the length of a Line
            #[wasm_bindgen(js_name = euclideanLength)]
            pub fn euclidean_length(&self) -> Data {
                Data::from_array(EuclideanLength::euclidean_length(&self.0))
            }
        }
    };
}

impl_euclidean_length!(PointData);
impl_euclidean_length!(MultiPointData);
impl_euclidean_length!(LineStringData);
impl_euclidean_length!(MultiLineStringData);

macro_rules! impl_vector {
    ($struct_name:ident) => {
        #[wasm_bindgen]
        impl $struct_name {
            /// Calculation of the length of a Line
            #[wasm_bindgen(js_name = euclideanLength)]
            pub fn euclidean_length(&self) -> WasmResult<Vector> {
                let chunks = EuclideanLength::euclidean_length(&self.0)?.chunk_refs();
                Ok(Vector::from_array_refs(chunks)?)
            }
        }
    };
}

impl_vector!(PointVector);
impl_vector!(LineStringVector);
impl_vector!(MultiPointVector);
impl_vector!(MultiLineStringVector);
