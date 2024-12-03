use geoarrow::scalar::OwnedMultiPolygon;
use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub struct MultiPolygon(pub(crate) OwnedMultiPolygon);

impl<'a> From<&'a MultiPolygon> for geoarrow::scalar::MultiPolygon<'a> {
    fn from(value: &'a MultiPolygon) -> Self {
        (&value.0).into()
    }
}

impl From<MultiPolygon> for geoarrow::scalar::OwnedMultiPolygon {
    fn from(value: MultiPolygon) -> Self {
        value.0
    }
}

impl<'a> From<geoarrow::scalar::MultiPolygon<'a>> for MultiPolygon {
    fn from(value: geoarrow::scalar::MultiPolygon<'a>) -> Self {
        MultiPolygon(value.into())
    }
}
