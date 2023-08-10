use crate::array::*;
use crate::broadcasting::BroadcastableFloat;
// use crate::scalar::Point;
use pyo3::prelude::*;

macro_rules! impl_skew {
    ($struct_name:ident) => {
        #[pymethods]
        impl $struct_name {
            /// An affine transformation which skews a geometry, sheared by a uniform angle along
            /// the x and y dimensions.
            pub fn skew(&self, degrees: BroadcastableFloat) -> Self {
                use geoarrow::algorithm::geo::Skew;
                Skew::skew(&self.0, degrees.0).into()
            }

            /// Skew a geometry from it's bounding box center, using different values for
            /// `x_factor` and `y_factor` to distort the geometry's [aspect
            /// ratio](https://en.wikipedia.org/wiki/Aspect_ratio).
            pub fn skew_xy(
                &self,
                degrees_x: BroadcastableFloat,
                degrees_y: BroadcastableFloat,
            ) -> Self {
                use geoarrow::algorithm::geo::Skew;
                Skew::skew_xy(&self.0, degrees_x.0, degrees_y.0).into()
            }

            // /// An affine transformation which skews a geometry around a point of `origin`, sheared
            // /// by an angle along the x and y dimensions.
            // ///
            // /// The point of origin is *usually* given as the 2D bounding box centre of the
            // /// geometry, in which case you can just use [`skew`](Self::skew) or
            // /// [`skew_xy`](Self::skew_xy), but this method allows you to specify any point.
            // pub fn skew_around_point(
            //     &self,
            //     degrees_x: BroadcastableFloat,
            //     degrees_y: BroadcastableFloat,
            //     origin: Point,
            // ) -> Self {
            //     use geoarrow::algorithm::geo::Skew;
            //     Skew::skew_around_point(&self.0, degrees_x.0, degrees_y.0, origin.0.into()).into()
            // }
        }
    };
}

impl_skew!(PointArray);
impl_skew!(LineStringArray);
impl_skew!(PolygonArray);
impl_skew!(MultiPointArray);
impl_skew!(MultiLineStringArray);
impl_skew!(MultiPolygonArray);
