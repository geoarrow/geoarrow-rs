use crate::array::*;
use crate::broadcasting::BroadcastableAffine;
use geoarrow::algorithm::broadcasting::BroadcastableVec;
use pyo3::prelude::*;

macro_rules! impl_rotate {
    ($struct_name:ident) => {
        #[pymethods]
        impl $struct_name {
            /// Apply an affine transformation like `scale`, `skew`, or `rotate` to an array of
            /// geometries.
            #[wasm_bindgen(js_name = affineTransform)]
            pub fn affine_transform(&self, transform: BroadcastableAffine) -> Self {
                use geoarrow::algorithm::geo::AffineOps;
                match transform.0 {
                    BroadcastableVec::Array(arr) => {
                        AffineOps::affine_transform(&self.0, &arr).into()
                    }
                    BroadcastableVec::Scalar(scalar) => {
                        AffineOps::affine_transform(&self.0, &scalar).into()
                    }
                }
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
