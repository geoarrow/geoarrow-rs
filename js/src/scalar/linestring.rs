use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub struct LineString(pub(crate) geoarrow::scalar::LineString);

impl From<LineString> for geoarrow::scalar::LineString {
    fn from(value: LineString) -> Self {
        value.0
    }
}

impl From<geoarrow::scalar::LineString> for LineString {
    fn from(value: geoarrow::scalar::LineString) -> Self {
        LineString(value)
    }
}
