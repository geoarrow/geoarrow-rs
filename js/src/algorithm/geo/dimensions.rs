use crate::data::*;
use crate::error::WasmResult;
use crate::vector::*;
use arrow_wasm::data::Data;
use arrow_wasm::vector::Vector;
use geoarrow::algorithm::geo::HasDimensions;
use wasm_bindgen::prelude::*;

macro_rules! impl_alg {
    ($struct_name:ident) => {
        #[wasm_bindgen]
        impl $struct_name {
            /// Some geometries, like a `MultiPoint`, can have zero coordinates - we call these
            /// `empty`.
            ///
            /// Types like `Point`, which have at least one coordinate by construction, can never
            /// be considered empty.
            #[wasm_bindgen(js_name = isEmpty)]
            pub fn is_empty(&self) -> Data {
                Data::from_array(HasDimensions::is_empty(&self.0))
            }
        }
    };
}

impl_alg!(PointData);
impl_alg!(LineStringData);
impl_alg!(PolygonData);
impl_alg!(MultiPointData);
impl_alg!(MultiLineStringData);
impl_alg!(MultiPolygonData);
impl_alg!(GeometryCollectionData);

macro_rules! impl_vector {
    ($struct_name:ident) => {
        #[wasm_bindgen]
        impl $struct_name {
            /// Some geometries, like a `MultiPoint`, can have zero coordinates - we call these
            /// `empty`.
            ///
            /// Types like `Point`, which have at least one coordinate by construction, can never
            /// be considered empty.
            #[wasm_bindgen(js_name = isEmpty)]
            pub fn is_empty(&self) -> WasmResult<Vector> {
                let chunks = HasDimensions::is_empty(&self.0)?.chunk_refs();
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
impl_vector!(GeometryCollectionVector);
