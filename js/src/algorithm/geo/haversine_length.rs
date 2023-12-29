use crate::data::*;
use arrow_wasm::arrow1::data::Float64Data;
use wasm_bindgen::prelude::*;

macro_rules! impl_haversine_length {
    ($struct_name:ident) => {
        #[wasm_bindgen]
        impl $struct_name {
            /// Determine the length of a geometry using the [haversine formula].
            ///
            /// [haversine formula]: https://en.wikipedia.org/wiki/Haversine_formula
            ///
            /// *Note*: this implementation uses a mean earth radius of 6371.088 km, based on the
            /// [recommendation of the IUGG](ftp://athena.fsv.cvut.cz/ZFG/grs80-Moritz.pdf)
            #[wasm_bindgen(js_name = haversineLength)]
            pub fn haversine_length(&self) -> Float64Data {
                use geoarrow::algorithm::geo::HaversineLength;
                HaversineLength::haversine_length(&self.0).into()
            }
        }
    };
}

impl_haversine_length!(PointData);
impl_haversine_length!(MultiPointData);
impl_haversine_length!(LineStringData);
impl_haversine_length!(MultiLineStringData);
