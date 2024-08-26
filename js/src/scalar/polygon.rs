use geoarrow::scalar::OwnedPolygon;
use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub struct Polygon(pub(crate) OwnedPolygon<i32, 2>);

impl<'a> From<&'a Polygon> for geoarrow::scalar::Polygon<'a, i32, 2> {
    fn from(value: &'a Polygon) -> Self {
        (&value.0).into()
    }
}

impl From<Polygon> for geoarrow::scalar::OwnedPolygon<i32, 2> {
    fn from(value: Polygon) -> Self {
        value.0
    }
}

impl<'a> From<geoarrow::scalar::Polygon<'a, i32, 2>> for Polygon {
    fn from(value: geoarrow::scalar::Polygon<'a, i32, 2>) -> Self {
        Polygon(value.into())
    }
}
