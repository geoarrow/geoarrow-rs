use crate::array::*;
use wasm_bindgen::prelude::*;

macro_rules! impl_contains {
    ($first:ty, $second:ty) => {
        #[wasm_bindgen]
        impl $first {
            /// Checks if `rhs` is completely contained within `self`.
            /// More formally, the interior of `rhs` has non-empty
            /// (set-theoretic) intersection but neither the interior,
            /// nor the boundary of `rhs` intersects the exterior of
            /// `self`. In other words, the [DE-9IM] intersection matrix
            /// of `(rhs, self)` is `T*F**F***`.
            ///
            /// [DE-9IM]: https://en.wikipedia.org/wiki/DE-9IM
            #[wasm_bindgen]
            pub fn contains(&self, other: &$second) -> BooleanArray {
                use geoarrow::algorithm::geo::Contains;
                BooleanArray(Contains::contains(&self.0, &other.0))
            }
        }
    };
}

// TODO: JS doesn't support function overloading
// Implementations on PointArray
impl_contains!(PointArray, PointArray);
impl_contains!(PointArray, LineStringArray);
impl_contains!(PointArray, PolygonArray);
impl_contains!(PointArray, MultiPointArray);
impl_contains!(PointArray, MultiLineStringArray);
impl_contains!(PointArray, MultiPolygonArray);
