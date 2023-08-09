use geoarrow::scalar::OwnedMultiLineString;
use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub struct MultiLineString(pub(crate) OwnedMultiLineString<i32>);

impl<'a> From<MultiLineString> for geoarrow::scalar::MultiLineString<'a, i32> {
    fn from(value: MultiLineString) -> Self {
        value.0.into()
    }
}

impl From<MultiLineString> for geoarrow::scalar::OwnedMultiLineString<i32> {
    fn from(value: MultiLineString) -> Self {
        value.0
    }
}

impl<'a> From<geoarrow::scalar::MultiLineString<'a, i32>> for MultiLineString {
    fn from(value: geoarrow::scalar::MultiLineString<'a, i32>) -> Self {
        MultiLineString(value.into())
    }
}
