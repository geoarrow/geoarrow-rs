use geoarrow::scalar::OwnedLineString;
use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub struct LineString(pub(crate) OwnedLineString);

impl<'a> From<&'a LineString> for geoarrow::scalar::LineString<'a> {
    fn from(value: &'a LineString) -> Self {
        (&value.0).into()
    }
}

impl From<LineString> for geoarrow::scalar::OwnedLineString {
    fn from(value: LineString) -> Self {
        value.0
    }
}

impl<'a> From<geoarrow::scalar::LineString<'a>> for LineString {
    fn from(value: geoarrow::scalar::LineString<'a>) -> Self {
        LineString(value.into())
    }
}
