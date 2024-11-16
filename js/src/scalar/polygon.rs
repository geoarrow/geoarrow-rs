use geoarrow::scalar::OwnedPolygon;
use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub struct Polygon(pub(crate) OwnedPolygon);

impl<'a> From<&'a Polygon> for geoarrow::scalar::Polygon<'a> {
    fn from(value: &'a Polygon) -> Self {
        (&value.0).into()
    }
}

impl From<Polygon> for geoarrow::scalar::OwnedPolygon {
    fn from(value: Polygon) -> Self {
        value.0
    }
}

impl<'a> From<geoarrow::scalar::Polygon<'a>> for Polygon {
    fn from(value: geoarrow::scalar::Polygon<'a>) -> Self {
        Polygon(value.into())
    }
}
