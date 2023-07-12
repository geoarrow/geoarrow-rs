use crate::array::*;
use wasm_bindgen::prelude::*;

macro_rules! impl_geodesic_length {
    ($struct_name:ident) => {
        #[wasm_bindgen]
        impl $struct_name {
            #[wasm_bindgen]
            pub fn geodesic_length(&self) -> FloatArray {
                use geoarrow::algorithm::geo::GeodesicLength;
                FloatArray(GeodesicLength::geodesic_length(&self.0))
            }
        }
    };
}

impl_geodesic_length!(PointArray);
impl_geodesic_length!(MultiPointArray);
impl_geodesic_length!(LineStringArray);
impl_geodesic_length!(MultiLineStringArray);
