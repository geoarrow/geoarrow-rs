use crate::data::*;
use crate::error::WasmResult;
use crate::vector::*;
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

macro_rules! impl_vector {
    ($struct_name:ident) => {
        #[wasm_bindgen]
        impl $struct_name {
            /// Return the bounding rectangle of a geometry
            #[wasm_bindgen(js_name = boundingRect)]
            pub fn bounding_rect(&self) -> WasmResult<RectVector> {
                use geoarrow::algorithm::geo::BoundingRect;
                Ok(RectVector(BoundingRect::bounding_rect(&self.0)?))
            }
        }
    };
}

impl_vector!(PointVector);
impl_vector!(LineStringVector);
impl_vector!(PolygonVector);
impl_vector!(MultiPointVector);
impl_vector!(MultiLineStringVector);
impl_vector!(MultiPolygonVector);
impl_vector!(MixedGeometryVector);
impl_vector!(GeometryCollectionVector);
