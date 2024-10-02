use geoarrow::scalar::OwnedMultiLineString;
use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub struct MultiLineString(pub(crate) OwnedMultiLineString<2>);

impl<'a> From<&'a MultiLineString> for geoarrow::scalar::MultiLineString<'a, 2> {
    fn from(value: &'a MultiLineString) -> Self {
        (&value.0).into()
    }
}

impl From<MultiLineString> for geoarrow::scalar::OwnedMultiLineString<2> {
    fn from(value: MultiLineString) -> Self {
        value.0
    }
}

impl<'a> From<geoarrow::scalar::MultiLineString<'a, 2>> for MultiLineString {
    fn from(value: geoarrow::scalar::MultiLineString<'a, 2>) -> Self {
        MultiLineString(value.into())
    }
}
