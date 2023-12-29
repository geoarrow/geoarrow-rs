use crate::broadcasting::BroadcastableFloat;
use crate::data::*;
use crate::scalar::Point;
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
                        Rotate::rotate_around_centroid(&self.0, &scalar).into()
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
                        Rotate::rotate_around_center(&self.0, &scalar).into()
                    }
                }
            }

            /// Rotate a Geometry around an arbitrary point by an angle, given in degrees
            ///
            /// Positive angles are counter-clockwise, and negative angles are clockwise rotations.
            #[wasm_bindgen(js_name = rotateAroundPoint)]
            pub fn rotate_around_point(&self, degrees: BroadcastableFloat, point: Point) -> Self {
                use geoarrow::algorithm::geo::Rotate;
                match degrees.0 {
                    BroadcastablePrimitive::Array(arr) => {
                        Rotate::rotate_around_point(&self.0, &arr, point.0.into()).into()
                    }
                    BroadcastablePrimitive::Scalar(scalar) => {
                        Rotate::rotate_around_point(&self.0, &scalar, point.0.into()).into()
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
