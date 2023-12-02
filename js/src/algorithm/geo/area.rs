use crate::array::*;
use wasm_bindgen::prelude::*;

macro_rules! impl_area {
    ($struct_name:ident) => {
        #[wasm_bindgen]
        impl $struct_name {
            /// Unsigned planar area of a geometry.
            #[wasm_bindgen]
            pub fn area(&self) -> FloatArray {
                use geoarrow::algorithm::geo::Area;
                FloatArray(Area::unsigned_area(&self.0))
            }

            /// Signed planar area of a geometry.
            #[wasm_bindgen(js_name = signedArea)]
            pub fn signed_area(&self) -> FloatArray {
                use geoarrow::algorithm::geo::Area;
                FloatArray(Area::signed_area(&self.0))
            }
        }
    };
}

impl_area!(PointArray);
impl_area!(LineStringArray);
impl_area!(PolygonArray);
impl_area!(MultiPointArray);
impl_area!(MultiLineStringArray);
impl_area!(MultiPolygonArray);