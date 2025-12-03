use geoarrow_schema::GeoArrowType;
use wasm_bindgen::prelude::*;

#[wasm_bindgen(js_name = GeoArrowType)]
pub struct JsGeoArrowType(GeoArrowType);

impl JsGeoArrowType {
    pub fn new(geoarrow_type: GeoArrowType) -> Self {
        Self(geoarrow_type)
    }

    pub(crate) fn inner(&self) -> &GeoArrowType {
        &self.0
    }
}

impl From<JsGeoArrowType> for GeoArrowType {
    fn from(value: JsGeoArrowType) -> Self {
        value.0
    }
}

impl From<GeoArrowType> for JsGeoArrowType {
    fn from(value: GeoArrowType) -> Self {
        Self(value)
    }
}
