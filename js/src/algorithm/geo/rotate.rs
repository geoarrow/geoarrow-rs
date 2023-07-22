use crate::array::*;
use crate::broadcasting::BroadcastableFloat;
use geoarrow::algorithm::broadcasting::BroadcastablePrimitive;
use wasm_bindgen::prelude::*;

macro_rules! impl_rotate {
    ($struct_name:ident) => {
        #[wasm_bindgen]
        impl $struct_name {
            /// Rotate a geometry around its centroid by an angle, in degrees
            ///
            /// Positive angles are counter-clockwise, and negative angles are clockwise rotations.
            #[wasm_bindgen(js_name = rotateAroundCentroid)]
            pub fn rotate_around_centroid(&self, degrees: BroadcastableFloat) -> Self {
                use geoarrow::algorithm::geo::Rotate;
                match degrees.0 {
                    BroadcastablePrimitive::Array(arr) => {
                        Rotate::rotate_around_centroid(&self.0, &arr).into()
                    }
                    BroadcastablePrimitive::Scalar(scalar) => {
                        Rotate::rotate_around_centroid(&self.0, scalar).into()
                    }
                }
            }

            /// Rotate a geometry around the center of its bounding box by an angle, in degrees.
            ///
            /// Positive angles are counter-clockwise, and negative angles are clockwise rotations.
            #[wasm_bindgen(js_name = rotateAroundCenter)]
            pub fn rotate_around_center(&self, degrees: BroadcastableFloat) -> Self {
                use geoarrow::algorithm::geo::Rotate;
                match degrees.0 {
                    BroadcastablePrimitive::Array(arr) => {
                        Rotate::rotate_around_center(&self.0, &arr).into()
                    }
                    BroadcastablePrimitive::Scalar(scalar) => {
                        Rotate::rotate_around_center(&self.0, scalar).into()
                    }
                }
            }

            // TODO: rotate around point
        }
    };
}

impl_rotate!(PointArray);
impl_rotate!(LineStringArray);
impl_rotate!(PolygonArray);
impl_rotate!(MultiPointArray);
impl_rotate!(MultiLineStringArray);
impl_rotate!(MultiPolygonArray);
