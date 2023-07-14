use crate::array::*;
use wasm_bindgen::prelude::*;

macro_rules! impl_simplify {
    ($struct_name:ident) => {
        #[wasm_bindgen]
        impl $struct_name {
            /// Simplifies a geometry.
            ///
            /// The [Ramer–Douglas–Peucker
            /// algorithm](https://en.wikipedia.org/wiki/Ramer–Douglas–Peucker_algorithm)
            /// simplifies a linestring. Polygons are simplified by running the RDP algorithm on
            /// all their constituent rings. This may result in invalid Polygons, and has no
            /// guarantee of preserving topology.
            ///
            /// Multi* objects are simplified by simplifying all their constituent geometries
            /// individually.
            ///
            /// An epsilon less than or equal to zero will return an unaltered version of the
            /// geometry.
            #[wasm_bindgen]
            pub fn simplify(&self, epsilon: f64) -> Self {
                use geoarrow::algorithm::geo::Simplify;
                Simplify::simplify(&self.0, &epsilon).into()
            }
        }
    };
}

impl_simplify!(PointArray);
impl_simplify!(LineStringArray);
impl_simplify!(PolygonArray);
impl_simplify!(MultiPointArray);
impl_simplify!(MultiLineStringArray);
impl_simplify!(MultiPolygonArray);
