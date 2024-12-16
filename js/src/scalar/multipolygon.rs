use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub struct MultiPolygon(pub(crate) geoarrow::scalar::MultiPolygon);

impl From<MultiPolygon> for geoarrow::scalar::MultiPolygon {
    fn from(value: MultiPolygon) -> Self {
        value.0
    }
}

impl From<geoarrow::scalar::MultiPolygon> for MultiPolygon {
    fn from(value: geoarrow::scalar::MultiPolygon) -> Self {
        MultiPolygon(value)
    }
}
