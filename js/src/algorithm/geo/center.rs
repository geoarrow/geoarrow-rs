use crate::data::*;
use crate::error::WasmResult;
use crate::vector::*;
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
            pub fn center(&self) -> PointData {
                use geoarrow::algorithm::geo::Center;
                PointData(Center::center(&self.0))
            }
        }
    };
}

impl_center!(PointData);
impl_center!(LineStringData);
impl_center!(PolygonData);
impl_center!(MultiPointData);
impl_center!(MultiLineStringData);
impl_center!(MultiPolygonData);
impl_center!(MixedGeometryData);
impl_center!(GeometryCollectionData);

macro_rules! impl_chunked {
    ($struct_name:ident) => {
        #[wasm_bindgen]
        impl $struct_name {
            /// Compute the center of geometries
            ///
            /// This first computes the axis-aligned bounding rectangle, then takes the center of
            /// that box
            #[wasm_bindgen]
            pub fn center(&self) -> WasmResult<PointVector> {
                use geoarrow::algorithm::geo::Center;
                Ok(PointVector(Center::center(&self.0)?))
            }
        }
    };
}

impl_chunked!(PointVector);
impl_chunked!(LineStringVector);
impl_chunked!(PolygonVector);
impl_chunked!(MultiPointVector);
impl_chunked!(MultiLineStringVector);
impl_chunked!(MultiPolygonVector);
impl_chunked!(MixedGeometryVector);
impl_chunked!(GeometryCollectionVector);
