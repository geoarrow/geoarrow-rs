use crate::broadcasting::BroadcastableFloat;
use crate::data::*;
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
            pub fn densify(&self, max_distance: BroadcastableFloat) -> Self {
                use geoarrow::algorithm::geo::Densify;
                Densify::densify(&self.0, max_distance.0).into()
            }
        }
    };
}

impl_densify!(LineStringData);
impl_densify!(PolygonData);
impl_densify!(MultiLineStringData);
impl_densify!(MultiPolygonData);
