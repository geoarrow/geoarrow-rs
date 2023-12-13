use crate::array::*;
use wasm_bindgen::prelude::*;

macro_rules! impl_bounding_rect {
    ($struct_name:ident) => {
        #[wasm_bindgen]
        impl $struct_name {
            /// Return the bounding rectangle of a geometry
            #[wasm_bindgen(js_name = boundingRect)]
            pub fn bounding_rect(&self) -> RectArray {
                use geoarrow::algorithm::geo::BoundingRect;
                RectArray(BoundingRect::bounding_rect(&self.0))
            }
        }
    };
}

impl_bounding_rect!(PointArray);
impl_bounding_rect!(LineStringArray);
impl_bounding_rect!(PolygonArray);
impl_bounding_rect!(MultiPointArray);
impl_bounding_rect!(MultiLineStringArray);
impl_bounding_rect!(MultiPolygonArray);
