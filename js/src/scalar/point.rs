use geoarrow::scalar::OwnedPoint;
use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub struct Point(pub(crate) OwnedPoint<2>);

impl<'a> From<Point> for geoarrow::scalar::Point<'a, 2> {
    fn from(value: Point) -> Self {
        value.0.into()
    }
}

impl From<Point> for geoarrow::scalar::OwnedPoint<2> {
    fn from(value: Point) -> Self {
        value.0
    }
}

impl<'a> From<geoarrow::scalar::Point<'a, 2>> for Point {
    fn from(value: geoarrow::scalar::Point<'a, 2>) -> Self {
        Point(value.into())
    }
}
