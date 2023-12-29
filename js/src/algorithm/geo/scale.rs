use crate::broadcasting::BroadcastableFloat;
use crate::data::*;
use wasm_bindgen::prelude::*;

macro_rules! impl_scale {
    ($struct_name:ident) => {
        #[wasm_bindgen]
        impl $struct_name {
            /// Scale a geometry from it's bounding box center.
            #[wasm_bindgen]
            pub fn scale(&self, scale_factor: BroadcastableFloat) -> Self {
                use geoarrow::algorithm::geo::Scale;
                Scale::scale(&self.0, scale_factor.0).into()
            }

            /// Scale a geometry from it's bounding box center, using different values for
            /// `x_factor` and `y_factor` to distort the geometry's [aspect
            /// ratio](https://en.wikipedia.org/wiki/Aspect_ratio).
            #[wasm_bindgen(js_name = scaleXy)]
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

impl_scale!(PointData);
impl_scale!(LineStringData);
impl_scale!(PolygonData);
impl_scale!(MultiPointData);
impl_scale!(MultiLineStringData);
impl_scale!(MultiPolygonData);
