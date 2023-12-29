use crate::broadcasting::BroadcastableAffine;
use crate::data::*;
use geoarrow::algorithm::broadcasting::BroadcastableVec;
use wasm_bindgen::prelude::*;

macro_rules! impl_rotate {
    ($struct_name:ident) => {
        #[wasm_bindgen]
        impl $struct_name {
            /// Apply an affine transformation like `scale`, `skew`, or `rotate` to an array of
            /// geometries.
            #[wasm_bindgen(js_name = affineTransform)]
            pub fn affine_transform(&self, transform: BroadcastableAffine) -> Self {
                use geoarrow::algorithm::geo::AffineOps;
                match transform.0 {
                    BroadcastableVec::Array(arr) => {
                        AffineOps::affine_transform(&self.0, arr.as_slice()).into()
                    }
                    BroadcastableVec::Scalar(scalar) => {
                        AffineOps::affine_transform(&self.0, &scalar).into()
                    }
                }
            }
        }
    };
}

impl_rotate!(PointData);
impl_rotate!(LineStringData);
impl_rotate!(PolygonData);
impl_rotate!(MultiPointData);
impl_rotate!(MultiLineStringData);
impl_rotate!(MultiPolygonData);
