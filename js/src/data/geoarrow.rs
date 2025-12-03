use std::sync::Arc;

use geoarrow_array::GeoArrowArray;
use wasm_bindgen::prelude::*;

#[wasm_bindgen(js_name = GeoArrowData)]
pub struct JsGeoArrowData(Arc<dyn GeoArrowArray>);

impl JsGeoArrowData {
    pub(crate) fn inner(&self) -> &Arc<dyn GeoArrowArray> {
        &self.0
    }

    pub(crate) fn into_inner(self) -> Arc<dyn GeoArrowArray> {
        self.0
    }
}
