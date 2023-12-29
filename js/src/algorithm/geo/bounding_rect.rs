use crate::data::*;
use wasm_bindgen::prelude::*;

macro_rules! impl_bounding_rect {
    ($struct_name:ident) => {
        #[wasm_bindgen]
        impl $struct_name {
            /// Return the bounding rectangle of a geometry
            #[wasm_bindgen(js_name = boundingRect)]
            pub fn bounding_rect(&self) -> RectData {
                use geoarrow::algorithm::geo::BoundingRect;
                RectData(BoundingRect::bounding_rect(&self.0))
            }
        }
    };
}

impl_bounding_rect!(PointData);
impl_bounding_rect!(LineStringData);
impl_bounding_rect!(PolygonData);
impl_bounding_rect!(MultiPointData);
impl_bounding_rect!(MultiLineStringData);
impl_bounding_rect!(MultiPolygonData);
impl_bounding_rect!(MixedGeometryData);
impl_bounding_rect!(GeometryCollectionData);
