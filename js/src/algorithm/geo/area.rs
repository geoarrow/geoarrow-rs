use crate::data::*;
use crate::error::WasmResult;
use crate::vector::*;
use arrow_wasm::data::Data;
use arrow_wasm::vector::Vector;
use geoarrow::algorithm::geo::Area;
use wasm_bindgen::prelude::*;

macro_rules! impl_area {
    ($struct_name:ident) => {
        #[wasm_bindgen]
        impl $struct_name {
            /// Unsigned planar area of a geometry.
            #[wasm_bindgen]
            pub fn area(&self) -> Data {
                Data::from_array(Area::unsigned_area(&self.0))
            }

            /// Signed planar area of a geometry.
            #[wasm_bindgen(js_name = signedArea)]
            pub fn signed_area(&self) -> Data {
                Data::from_array(Area::signed_area(&self.0))
            }
        }
    };
}

impl_area!(PointData);
impl_area!(LineStringData);
impl_area!(PolygonData);
impl_area!(MultiPointData);
impl_area!(MultiLineStringData);
impl_area!(MultiPolygonData);
impl_area!(MixedGeometryData);
impl_area!(GeometryCollectionData);

macro_rules! impl_vector {
    ($struct_name:ident) => {
        #[wasm_bindgen]
        impl $struct_name {
            /// Unsigned planar area of a geometry.
            #[wasm_bindgen]
            pub fn area(&self) -> WasmResult<Vector> {
                let chunks = Area::unsigned_area(&self.0)?.chunk_refs();
                Ok(Vector::from_array_refs(chunks)?)
            }

            /// Signed planar area of a geometry.
            #[wasm_bindgen(js_name = signedArea)]
            pub fn signed_area(&self) -> WasmResult<Vector> {
                let chunks = Area::signed_area(&self.0)?.chunk_refs();
                Ok(Vector::from_array_refs(chunks)?)
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
