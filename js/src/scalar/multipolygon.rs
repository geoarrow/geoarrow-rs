use geoarrow::scalar::OwnedMultiPolygon;
use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub struct MultiPolygon(pub(crate) OwnedMultiPolygon<i32>);

impl<'a> From<MultiPolygon> for geoarrow::scalar::MultiPolygon<'a, i32> {
    fn from(value: MultiPolygon) -> Self {
        value.0.into()
    }
}

impl From<MultiPolygon> for geoarrow::scalar::OwnedMultiPolygon<i32> {
    fn from(value: MultiPolygon) -> Self {
        value.0
    }
}

impl<'a> From<geoarrow::scalar::MultiPolygon<'a, i32>> for MultiPolygon {
    fn from(value: geoarrow::scalar::MultiPolygon<'a, i32>) -> Self {
        MultiPolygon(value.into())
    }
}
