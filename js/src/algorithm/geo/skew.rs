use crate::array::*;
use crate::broadcasting::BroadcastableFloat;
use wasm_bindgen::prelude::*;

macro_rules! impl_skew {
    ($struct_name:ident) => {
        #[wasm_bindgen]
        impl $struct_name {
            /// An affine transformation which skews a geometry, sheared by a uniform angle along
            /// the x and y dimensions.
            #[wasm_bindgen]
            pub fn skew(&self, degrees: BroadcastableFloat) -> Self {
                use geoarrow::algorithm::geo::Skew;
                Skew::skew(&self.0, degrees.0).into()
            }

            /// Skew a geometry from it's bounding box center, using different values for
            /// `x_factor` and `y_factor` to distort the geometry's [aspect
            /// ratio](https://en.wikipedia.org/wiki/Aspect_ratio).
            #[wasm_bindgen(js_name = skewXy)]
            pub fn skew_xy(
                &self,
                degrees_x: BroadcastableFloat,
                degrees_y: BroadcastableFloat,
            ) -> Self {
                use geoarrow::algorithm::geo::Skew;
                Skew::skew_xy(&self.0, degrees_x.0, degrees_y.0).into()
            }

            // TODO: skew around point
        }
    };
}

impl_skew!(PointArray);
impl_skew!(LineStringArray);
impl_skew!(PolygonArray);
impl_skew!(MultiPointArray);
impl_skew!(MultiLineStringArray);
impl_skew!(MultiPolygonArray);
