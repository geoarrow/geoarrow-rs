use geoarrow::scalar::OwnedPolygon;
use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub struct Polygon(pub(crate) OwnedPolygon<i32>);

impl<'a> From<Polygon> for geoarrow::scalar::Polygon<'a, i32> {
    fn from(value: Polygon) -> Self {
        value.0.into()
    }
}

impl From<Polygon> for geoarrow::scalar::OwnedPolygon<i32> {
    fn from(value: Polygon) -> Self {
        value.0
    }
}

impl<'a> From<geoarrow::scalar::Polygon<'a, i32>> for Polygon {
    fn from(value: geoarrow::scalar::Polygon<'a, i32>) -> Self {
        Polygon(value.into())
    }
}
