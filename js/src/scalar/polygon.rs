use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub struct Polygon(pub(crate) geoarrow::scalar::Polygon);

impl From<Polygon> for geoarrow::scalar::Polygon {
    fn from(value: Polygon) -> Self {
        value.0
    }
}

impl From<geoarrow::scalar::Polygon> for Polygon {
    fn from(value: geoarrow::scalar::Polygon) -> Self {
        Polygon(value)
    }
}
