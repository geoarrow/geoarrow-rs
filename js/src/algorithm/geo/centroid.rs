use crate::data::*;
use crate::error::WasmResult;
use crate::vector::*;
use wasm_bindgen::prelude::*;

macro_rules! impl_centroid {
    ($struct_name:ident) => {
        #[wasm_bindgen]
        impl $struct_name {
            /// Calculation of the centroid.
            ///
            /// The centroid is the arithmetic mean position of all points in the shape.
            /// Informally, it is the point at which a cutout of the shape could be perfectly
            /// balanced on the tip of a pin.
            ///
            /// The geometric centroid of a convex object always lies in the object.
            /// A non-convex object might have a centroid that _is outside the object itself_.
            #[wasm_bindgen]
            pub fn centroid(&self) -> PointData {
                use geoarrow::algorithm::geo::Centroid;
                PointData(Centroid::centroid(&self.0))
            }
        }
    };
}

impl_centroid!(PointData);
impl_centroid!(LineStringData);
impl_centroid!(PolygonData);
impl_centroid!(MultiPointData);
impl_centroid!(MultiLineStringData);
impl_centroid!(MultiPolygonData);
impl_centroid!(MixedGeometryData);
impl_centroid!(GeometryCollectionData);

macro_rules! impl_chunked {
    ($struct_name:ident) => {
        #[wasm_bindgen]
        impl $struct_name {
            /// Calculation of the centroid.
            ///
            /// The centroid is the arithmetic mean position of all points in the shape.
            /// Informally, it is the point at which a cutout of the shape could be perfectly
            /// balanced on the tip of a pin.
            ///
            /// The geometric centroid of a convex object always lies in the object.
            /// A non-convex object might have a centroid that _is outside the object itself_.
            #[wasm_bindgen]
            pub fn centroid(&self) -> WasmResult<PointVector> {
                use geoarrow::algorithm::geo::Centroid;
                Ok(PointVector(Centroid::centroid(&self.0)?))
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
