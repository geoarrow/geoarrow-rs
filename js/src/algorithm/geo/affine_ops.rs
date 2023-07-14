use crate::array::*;
use crate::broadcasting::BroadcastableAffine;
use wasm_bindgen::prelude::*;

macro_rules! impl_rotate {
    ($struct_name:ident) => {
        #[wasm_bindgen]
        impl $struct_name {
            /// Apply an affine transformation like `scale`, `skew`, or `rotate` to an array of
            /// geometries.
            #[wasm_bindgen]
            pub fn affine_transform(&self, transform: BroadcastableAffine) -> Self {
                use geoarrow::algorithm::geo::AffineOps;
                AffineOps::affine_transform(&self.0, transform.0).into()
            }
        }
    };
}

impl_rotate!(PointArray);
impl_rotate!(LineStringArray);
impl_rotate!(PolygonArray);
impl_rotate!(MultiPointArray);
impl_rotate!(MultiLineStringArray);
impl_rotate!(MultiPolygonArray);
