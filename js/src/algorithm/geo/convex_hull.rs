use crate::array::*;
use wasm_bindgen::prelude::*;

macro_rules! impl_alg {
    ($struct_name:ident) => {
        #[wasm_bindgen]
        impl $struct_name {
            /// Returns the convex hull of a Polygon. The hull is always oriented
            /// counter-clockwise.
            ///
            /// This implementation uses the QuickHull algorithm, based on [Barber, C. Bradford;
            /// Dobkin, David P.; Huhdanpaa, Hannu (1 December
            /// 1996)](https://dx.doi.org/10.1145%2F235815.235821) Original paper here:
            /// <http://www.cs.princeton.edu/~dpd/Papers/BarberDobkinHuhdanpaa.pdf>
            #[wasm_bindgen(js_name = convexHull)]
            pub fn convex_hull(&self) -> PolygonArray {
                use geoarrow::algorithm::geo::ConvexHull;
                PolygonArray(ConvexHull::convex_hull(&self.0))
            }
        }
    };
}

impl_alg!(PointArray);
impl_alg!(LineStringArray);
impl_alg!(PolygonArray);
impl_alg!(MultiPointArray);
impl_alg!(MultiLineStringArray);
impl_alg!(MultiPolygonArray);
