use crate::data::*;
use arrow_wasm::arrow1::data::Float64Data;
use wasm_bindgen::prelude::*;

macro_rules! impl_geodesic_length {
    ($struct_name:ident) => {
        #[wasm_bindgen]
        impl $struct_name {
            /// Determine the length of a geometry on an ellipsoidal model of the earth.
            ///
            /// This uses the geodesic measurement methods given by [Karney (2013)]. As opposed to
            /// older methods like Vincenty, this method is accurate to a few nanometers and always
            /// converges.
            ///
            /// [Karney (2013)]:  https://arxiv.org/pdf/1109.4448.pdf
            #[wasm_bindgen(js_name = geodesicLength)]
            pub fn geodesic_length(&self) -> Float64Data {
                use geoarrow::algorithm::geo::GeodesicLength;
                GeodesicLength::geodesic_length(&self.0).into()
            }
        }
    };
}

impl_geodesic_length!(PointData);
impl_geodesic_length!(MultiPointData);
impl_geodesic_length!(LineStringData);
impl_geodesic_length!(MultiLineStringData);
