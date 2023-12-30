use crate::data::*;
use crate::vector::*;
use arrow_wasm::arrow1::data::Float64Data;
use arrow_wasm::arrow1::vector::Float64Vector;
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

macro_rules! impl_vector {
    ($struct_name:ident) => {
        #[wasm_bindgen]
        impl $struct_name {
            /// Calculation of the length of a Line
            #[wasm_bindgen(js_name = euclideanLength)]
            pub fn euclidean_length(&self) -> Float64Vector {
                use geoarrow::algorithm::geo::EuclideanLength;
                Float64Vector::new(
                    EuclideanLength::euclidean_length(&self.0)
                        .unwrap()
                        .into_inner(),
                )
            }
        }
    };
}

impl_vector!(PointVector);
impl_vector!(LineStringVector);
impl_vector!(MultiPointVector);
impl_vector!(MultiLineStringVector);
