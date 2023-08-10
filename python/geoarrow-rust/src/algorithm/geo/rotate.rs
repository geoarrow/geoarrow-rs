use crate::array::*;
use crate::broadcasting::BroadcastableFloat;
// use crate::scalar::Point;
use geoarrow::algorithm::broadcasting::BroadcastablePrimitive;
use pyo3::prelude::*;

macro_rules! impl_rotate {
    ($struct_name:ident) => {
        #[pymethods]
        impl $struct_name {
            /// Rotate a geometry around its centroid by an angle, in degrees
            ///
            /// Positive angles are counter-clockwise, and negative angles are clockwise rotations.
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

            // /// Rotate a Geometry around an arbitrary point by an angle, given in degrees
            // ///
            // /// Positive angles are counter-clockwise, and negative angles are clockwise rotations.
            // pub fn rotate_around_point(&self, degrees: BroadcastableFloat, point: Point) -> Self {
            //     use geoarrow::algorithm::geo::Rotate;
            //     match degrees.0 {
            //         BroadcastablePrimitive::Array(arr) => {
            //             Rotate::rotate_around_point(&self.0, &arr, point.0.into()).into()
            //         }
            //         BroadcastablePrimitive::Scalar(scalar) => {
            //             Rotate::rotate_around_point(&self.0, &scalar, point.0.into()).into()
            //         }
            //     }
            // }
        }
    };
}

impl_rotate!(PointArray);
impl_rotate!(LineStringArray);
impl_rotate!(PolygonArray);
impl_rotate!(MultiPointArray);
impl_rotate!(MultiLineStringArray);
impl_rotate!(MultiPolygonArray);
