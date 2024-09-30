use geoarrow::scalar::OwnedLineString;
use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub struct LineString(pub(crate) OwnedLineString<2>);

impl<'a> From<&'a LineString> for geoarrow::scalar::LineString<'a, 2> {
    fn from(value: &'a LineString) -> Self {
        (&value.0).into()
    }
}

impl From<LineString> for geoarrow::scalar::OwnedLineString<2> {
    fn from(value: LineString) -> Self {
        value.0
    }
}

impl<'a> From<geoarrow::scalar::LineString<'a, 2>> for LineString {
    fn from(value: geoarrow::scalar::LineString<'a, 2>) -> Self {
        LineString(value.into())
    }
}
