use crate::data::*;
use crate::vector::*;
use wasm_bindgen::prelude::*;

macro_rules! impl_densify {
    ($struct_name:ident) => {
        #[wasm_bindgen]
        impl $struct_name {
            /// Return a new linear geometry containing both existing and new interpolated
            /// coordinates with a maximum distance of `max_distance` between them.
            ///
            /// Note: `max_distance` must be greater than 0.
            #[wasm_bindgen]
            pub fn densify(&self, max_distance: f64) -> Self {
                use geoarrow::algorithm::geo::Densify;
                Densify::densify(&self.0, max_distance).into()
            }
        }
    };
}

impl_densify!(LineStringData);
impl_densify!(PolygonData);
impl_densify!(MultiLineStringData);
impl_densify!(MultiPolygonData);

macro_rules! impl_vector {
    ($struct_name:ident) => {
        #[wasm_bindgen]
        impl $struct_name {
            /// Return a new linear geometry containing both existing and new interpolated
            /// coordinates with a maximum distance of `max_distance` between them.
            ///
            /// Note: `max_distance` must be greater than 0.
            #[wasm_bindgen]
            pub fn densify(&self, max_distance: f64) -> Self {
                use geoarrow::algorithm::geo::Densify;
                Densify::densify(&self.0, max_distance).into()
            }
        }
    };
}

impl_vector!(LineStringVector);
impl_vector!(PolygonVector);
impl_vector!(MultiLineStringVector);
impl_vector!(MultiPolygonVector);
