use crate::array::*;
use pyo3::prelude::*;

macro_rules! impl_simplify_vw {
    ($struct_name:ident) => {
        #[pymethods]
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
            pub fn simplify_vw(&self, epsilon: f64) -> Self {
                use geoarrow::algorithm::geo::SimplifyVw;
                SimplifyVw::simplify_vw(&self.0, &epsilon).into()
            }
        }
    };
}

impl_simplify_vw!(PointArray);
impl_simplify_vw!(LineStringArray);
impl_simplify_vw!(PolygonArray);
impl_simplify_vw!(MultiPointArray);
impl_simplify_vw!(MultiLineStringArray);
impl_simplify_vw!(MultiPolygonArray);
