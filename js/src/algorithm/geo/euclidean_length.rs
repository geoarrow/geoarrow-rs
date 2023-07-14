use crate::array::*;
use wasm_bindgen::prelude::*;

macro_rules! impl_euclidean_length {
    ($struct_name:ident) => {
        #[wasm_bindgen]
        impl $struct_name {
            /// Calculation of the length of a Line
            #[wasm_bindgen(js_name = euclideanLength)]
            pub fn euclidean_length(&self) -> FloatArray {
                use geoarrow::algorithm::geo::EuclideanLength;
                FloatArray(EuclideanLength::euclidean_length(&self.0))
            }
        }
    };
}

impl_euclidean_length!(PointArray);
impl_euclidean_length!(MultiPointArray);
impl_euclidean_length!(LineStringArray);
impl_euclidean_length!(MultiLineStringArray);
