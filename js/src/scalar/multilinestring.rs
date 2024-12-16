use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub struct MultiLineString(pub(crate) geoarrow::scalar::MultiLineString);

impl From<MultiLineString> for geoarrow::scalar::MultiLineString {
    fn from(value: MultiLineString) -> Self {
        value.0
    }
}

impl From<geoarrow::scalar::MultiLineString> for MultiLineString {
    fn from(value: geoarrow::scalar::MultiLineString) -> Self {
        MultiLineString(value)
    }
}
