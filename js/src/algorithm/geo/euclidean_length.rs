use crate::data::*;
use arrow_wasm::arrow1::data::Float64Data;
use wasm_bindgen::prelude::*;

macro_rules! impl_euclidean_length {
    ($struct_name:ident) => {
        #[wasm_bindgen]
        impl $struct_name {
            /// Calculation of the length of a Line
            #[wasm_bindgen(js_name = euclideanLength)]
            pub fn euclidean_length(&self) -> Float64Data {
                use geoarrow::algorithm::geo::EuclideanLength;
                Float64Data::new(EuclideanLength::euclidean_length(&self.0))
            }
        }
    };
}

impl_euclidean_length!(PointData);
impl_euclidean_length!(MultiPointData);
impl_euclidean_length!(LineStringData);
impl_euclidean_length!(MultiLineStringData);
