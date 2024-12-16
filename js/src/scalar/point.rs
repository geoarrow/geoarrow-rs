use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub struct Point(pub(crate) geoarrow::scalar::Point);

impl From<Point> for geoarrow::scalar::Point {
    fn from(value: Point) -> Self {
        value.0
    }
}

impl From<geoarrow::scalar::Point> for Point {
    fn from(value: geoarrow::scalar::Point) -> Self {
        Point(value)
    }
}
