use geoarrow::scalar::OwnedLineString;
use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub struct LineString(pub(crate) OwnedLineString<i32>);

impl<'a> From<LineString> for geoarrow::scalar::LineString<'a, i32> {
    fn from(value: LineString) -> Self {
        value.0.into()
    }
}

impl<'a> From<geoarrow::scalar::LineString<'a, i32>> for LineString {
    fn from(value: geoarrow::scalar::LineString<'a, i32>) -> Self {
        LineString(value.into())
    }
}
