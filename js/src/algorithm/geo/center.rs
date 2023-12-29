use crate::array::*;
use crate::chunked_array::*;
use crate::error::WasmResult;
use wasm_bindgen::prelude::*;

macro_rules! impl_center {
    ($struct_name:ident) => {
        #[wasm_bindgen]
        impl $struct_name {
            /// Compute the center of geometries
            ///
            /// This first computes the axis-aligned bounding rectangle, then takes the center of
            /// that box
            #[wasm_bindgen]
            pub fn center(&self) -> PointArray {
                use geoarrow::algorithm::geo::Center;
                PointArray(Center::center(&self.0))
            }
        }
    };
}

impl_center!(PointArray);
impl_center!(LineStringArray);
impl_center!(PolygonArray);
impl_center!(MultiPointArray);
impl_center!(MultiLineStringArray);
impl_center!(MultiPolygonArray);
impl_center!(MixedGeometryArray);
impl_center!(GeometryCollectionArray);
impl_center!(GeometryArray);

macro_rules! impl_chunked {
    ($struct_name:ident) => {
        #[wasm_bindgen]
        impl $struct_name {
            /// Compute the center of geometries
            ///
            /// This first computes the axis-aligned bounding rectangle, then takes the center of
            /// that box
            #[wasm_bindgen]
            pub fn center(&self) -> WasmResult<ChunkedPointArray> {
                use geoarrow::algorithm::geo::Center;
                Ok(ChunkedPointArray(Center::center(&self.0)?))
            }
        }
    };
}

impl_chunked!(ChunkedPointArray);
impl_chunked!(ChunkedLineStringArray);
impl_chunked!(ChunkedPolygonArray);
impl_chunked!(ChunkedMultiPointArray);
impl_chunked!(ChunkedMultiLineStringArray);
impl_chunked!(ChunkedMultiPolygonArray);
impl_chunked!(ChunkedMixedGeometryArray);
impl_chunked!(ChunkedGeometryCollectionArray);
