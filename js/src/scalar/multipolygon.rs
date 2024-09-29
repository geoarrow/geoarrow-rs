use geoarrow::scalar::OwnedMultiPolygon;
use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub struct MultiPolygon(pub(crate) OwnedMultiPolygon<2>);

impl<'a> From<&'a MultiPolygon> for geoarrow::scalar::MultiPolygon<'a, i32, 2> {
    fn from(value: &'a MultiPolygon) -> Self {
        (&value.0).into()
    }
}

impl From<MultiPolygon> for geoarrow::scalar::OwnedMultiPolygon<2> {
    fn from(value: MultiPolygon) -> Self {
        value.0
    }
}

impl<'a> From<geoarrow::scalar::MultiPolygon<'a, i32, 2>> for MultiPolygon {
    fn from(value: geoarrow::scalar::MultiPolygon<'a, i32, 2>) -> Self {
        MultiPolygon(value.into())
    }
}
