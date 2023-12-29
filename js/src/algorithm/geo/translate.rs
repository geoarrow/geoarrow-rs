use crate::broadcasting::BroadcastableFloat;
use crate::data::*;
use wasm_bindgen::prelude::*;

macro_rules! impl_translate {
    ($struct_name:ident) => {
        #[wasm_bindgen]
        impl $struct_name {
            /// Translate a Geometry along its axes by the given offsets
            #[wasm_bindgen]
            pub fn translate(
                &self,
                x_offset: BroadcastableFloat,
                y_offset: BroadcastableFloat,
            ) -> Self {
                use geoarrow::algorithm::geo::Translate;
                Translate::translate(&self.0, x_offset.0, y_offset.0).into()
            }
        }
    };
}

impl_translate!(PointData);
impl_translate!(LineStringData);
impl_translate!(PolygonData);
impl_translate!(MultiPointData);
impl_translate!(MultiLineStringData);
impl_translate!(MultiPolygonData);
