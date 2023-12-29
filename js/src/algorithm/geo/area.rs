use crate::array::*;
use crate::chunked_array::*;
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
impl_area!(MixedGeometryArray);
impl_area!(GeometryCollectionArray);
impl_area!(GeometryArray);

macro_rules! impl_chunked_area {
    ($struct_name:ident) => {
        #[wasm_bindgen]
        impl $struct_name {
            /// Unsigned planar area of a geometry.
            #[wasm_bindgen]
            pub fn area(&self) -> ChunkedFloat64Array {
                use geoarrow::algorithm::geo::Area;
                Area::unsigned_area(&self.0).unwrap().into()
            }

            /// Signed planar area of a geometry.
            #[wasm_bindgen(js_name = signedArea)]
            pub fn signed_area(&self) -> ChunkedFloat64Array {
                use geoarrow::algorithm::geo::Area;
                Area::signed_area(&self.0).unwrap().into()
            }
        }
    };
}

impl_chunked_area!(ChunkedPointArray);
impl_chunked_area!(ChunkedLineStringArray);
impl_chunked_area!(ChunkedPolygonArray);
impl_chunked_area!(ChunkedMultiPointArray);
impl_chunked_area!(ChunkedMultiLineStringArray);
impl_chunked_area!(ChunkedMultiPolygonArray);
impl_chunked_area!(ChunkedMixedGeometryArray);
impl_chunked_area!(ChunkedGeometryCollectionArray);
