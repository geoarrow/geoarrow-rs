use crate::data::*;
use wasm_bindgen::prelude::*;

macro_rules! impl_simplify_vw {
    ($struct_name:ident) => {
        #[wasm_bindgen]
        impl $struct_name {
            /// Returns the simplified representation of a geometry, using the
            /// [Visvalingam-Whyatt](http://www.tandfonline.com/doi/abs/10.1179/000870493786962263)
            /// algorithm
            ///
            /// See [here](https://bost.ocks.org/mike/simplify/) for a graphical explanation
            ///
            /// Polygons are simplified by running the algorithm on all their constituent rings.
            /// This may result in invalid Polygons, and has no guarantee of preserving topology.
            /// Multi* objects are simplified by simplifying all their constituent geometries
            /// individually.
            ///
            /// An epsilon less than or equal to zero will return an unaltered version of the
            /// geometry.
            #[wasm_bindgen(js_name = simplifyVw)]
            pub fn simplify_vw(&self, epsilon: f64) -> Self {
                use geoarrow::algorithm::geo::SimplifyVw;
                SimplifyVw::simplify_vw(&self.0, &epsilon).into()
            }
        }
    };
}

impl_simplify_vw!(PointData);
impl_simplify_vw!(LineStringData);
impl_simplify_vw!(PolygonData);
impl_simplify_vw!(MultiPointData);
impl_simplify_vw!(MultiLineStringData);
impl_simplify_vw!(MultiPolygonData);
