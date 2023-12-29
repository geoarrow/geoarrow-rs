use crate::data::*;
use arrow_wasm::arrow1::data::BooleanData;
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
            #[wasm_bindgen(js_name = isEmpty)]
            pub fn is_empty(&self) -> BooleanData {
                use geoarrow::algorithm::geo::HasDimensions;
                BooleanData::new(HasDimensions::is_empty(&self.0))
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
