use geoarrow::scalar::OwnedPoint;
use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub struct Point(pub(crate) OwnedPoint);

impl<'a> From<Point> for geoarrow::scalar::Point<'a> {
    fn from(value: Point) -> Self {
        value.0.into()
    }
}

impl<'a> From<geoarrow::scalar::Point<'a>> for Point {
    fn from(value: geoarrow::scalar::Point<'a>) -> Self {
        Point(value.into())
    }
}
