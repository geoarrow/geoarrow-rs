use crate::data::*;
use crate::vector::*;
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
            pub fn convex_hull(&self) -> PolygonData {
                use geoarrow::algorithm::geo::ConvexHull;
                PolygonData(ConvexHull::convex_hull(&self.0))
            }
        }
    };
}

impl_alg!(PointData);
impl_alg!(LineStringData);
impl_alg!(PolygonData);
impl_alg!(MultiPointData);
impl_alg!(MultiLineStringData);
impl_alg!(MultiPolygonData);
impl_alg!(MixedGeometryData);
impl_alg!(GeometryCollectionData);

macro_rules! impl_vector {
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
            pub fn convex_hull(&self) -> PolygonVector {
                use geoarrow::algorithm::geo::ConvexHull;
                PolygonVector(ConvexHull::convex_hull(&self.0).unwrap())
            }
        }
    };
}

impl_vector!(PointVector);
impl_vector!(LineStringVector);
impl_vector!(PolygonVector);
impl_vector!(MultiPointVector);
impl_vector!(MultiLineStringVector);
impl_vector!(MultiPolygonVector);
impl_vector!(MixedGeometryVector);
impl_vector!(GeometryCollectionVector);
