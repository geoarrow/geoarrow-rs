use crate::broadcasting::BroadcastableFloat;
use crate::data::*;
use crate::scalar::Point;
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

            /// An affine transformation which skews a geometry around a point of `origin`, sheared
            /// by an angle along the x and y dimensions.
            ///
            /// The point of origin is *usually* given as the 2D bounding box centre of the
            /// geometry, in which case you can just use [`skew`](Self::skew) or
            /// [`skew_xy`](Self::skew_xy), but this method allows you to specify any point.
            #[wasm_bindgen(js_name = skewAroundPoint)]
            pub fn skew_around_point(
                &self,
                degrees_x: BroadcastableFloat,
                degrees_y: BroadcastableFloat,
                origin: Point,
            ) -> Self {
                use geoarrow::algorithm::geo::Skew;
                Skew::skew_around_point(&self.0, degrees_x.0, degrees_y.0, origin.0.into()).into()
            }
        }
    };
}

impl_skew!(PointData);
impl_skew!(LineStringData);
impl_skew!(PolygonData);
impl_skew!(MultiPointData);
impl_skew!(MultiLineStringData);
impl_skew!(MultiPolygonData);
