use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub struct MultiPoint(pub(crate) geoarrow::scalar::MultiPoint);

impl From<MultiPoint> for geoarrow::scalar::MultiPoint {
    fn from(value: MultiPoint) -> Self {
        value.0
    }
}

impl From<geoarrow::scalar::MultiPoint> for MultiPoint {
    fn from(value: geoarrow::scalar::MultiPoint) -> Self {
        MultiPoint(value)
    }
}
