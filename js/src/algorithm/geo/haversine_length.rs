use crate::array::*;
use wasm_bindgen::prelude::*;

macro_rules! impl_haversine_length {
    ($struct_name:ident) => {
        #[wasm_bindgen]
        impl $struct_name {
            #[wasm_bindgen]
            pub fn haversine_length(&self) -> FloatArray {
                use geoarrow::algorithm::geo::HaversineLength;
                FloatArray(HaversineLength::haversine_length(&self.0))
            }
        }
    };
}

impl_haversine_length!(PointArray);
impl_haversine_length!(MultiPointArray);
impl_haversine_length!(LineStringArray);
impl_haversine_length!(MultiLineStringArray);
