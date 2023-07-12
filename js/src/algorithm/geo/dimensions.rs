use crate::array::*;
use wasm_bindgen::prelude::*;

macro_rules! impl_alg {
    ($struct_name:ident) => {
        #[wasm_bindgen]
        impl $struct_name {
            /// Some geometries, like a `MultiPoint`, can have zero coordinates - we call these
            /// `empty`.
            ///
            /// Types like `Point`, which have at least one coordinate by construction, can never
            /// be considered empty.
            #[wasm_bindgen]
            pub fn is_empty(&self) -> BooleanArray {
                use geoarrow::algorithm::geo::HasDimensions;
                BooleanArray(HasDimensions::is_empty(&self.0))
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
impl_alg!(GeometryArray);
