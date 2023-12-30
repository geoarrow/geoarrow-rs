use crate::data::*;
use crate::vector::*;
use arrow_wasm::arrow1::data::Float64Data;
use arrow_wasm::arrow1::vector::Float64Vector;
use wasm_bindgen::prelude::*;

macro_rules! impl_area {
    ($struct_name:ident) => {
        #[wasm_bindgen]
        impl $struct_name {
            /// Unsigned planar area of a geometry.
            #[wasm_bindgen]
            pub fn area(&self) -> Float64Data {
                use geoarrow::algorithm::geo::Area;
                Area::unsigned_area(&self.0).into()
            }

            /// Signed planar area of a geometry.
            #[wasm_bindgen(js_name = signedArea)]
            pub fn signed_area(&self) -> Float64Data {
                use geoarrow::algorithm::geo::Area;
                Area::signed_area(&self.0).into()
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
            pub fn area(&self) -> Float64Vector {
                use geoarrow::algorithm::geo::Area;
                Float64Vector::new(Area::unsigned_area(&self.0).unwrap().into_inner())
            }

            /// Signed planar area of a geometry.
            #[wasm_bindgen(js_name = signedArea)]
            pub fn signed_area(&self) -> Float64Vector {
                use geoarrow::algorithm::geo::Area;
                Float64Vector::new(Area::signed_area(&self.0).unwrap().into_inner())
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
