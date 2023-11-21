use crate::array::*;
use crate::broadcasting::BroadcastableFloat;
use pyo3::prelude::*;

macro_rules! impl_scale {
    ($struct_name:ident) => {
        #[pymethods]
        impl $struct_name {
            /// Scale a geometry from it's bounding box center.
            pub fn scale(&self, scale_factor: BroadcastableFloat) -> Self {
                use geoarrow::algorithm::geo::Scale;
                Scale::scale(&self.0, scale_factor.0).into()
            }

            /// Scale a geometry from it's bounding box center, using different values for
            /// `x_factor` and `y_factor` to distort the geometry's [aspect
            /// ratio](https://en.wikipedia.org/wiki/Aspect_ratio).
            pub fn scale_xy(
                &self,
                x_factor: BroadcastableFloat,
                y_factor: BroadcastableFloat,
            ) -> Self {
                use geoarrow::algorithm::geo::Scale;
                Scale::scale_xy(&self.0, x_factor.0, y_factor.0).into()
            }

            // TODO: scale around point
        }
    };
}

impl_scale!(PointArray);
impl_scale!(LineStringArray);
impl_scale!(PolygonArray);
impl_scale!(MultiPointArray);
impl_scale!(MultiLineStringArray);
impl_scale!(MultiPolygonArray);
