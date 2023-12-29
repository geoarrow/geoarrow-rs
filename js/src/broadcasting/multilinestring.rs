use crate::data::MultiLineStringData;
use crate::scalar::MultiLineString;
use wasm_bindgen::prelude::*;

enum _BroadcastableMultiLineString {
    Scalar(geoarrow::scalar::OwnedMultiLineString<i32>),
    Array(geoarrow::array::MultiLineStringArray<i32>),
}

#[wasm_bindgen]
pub struct BroadcastableMultiLineString(_BroadcastableMultiLineString);

#[wasm_bindgen]
impl BroadcastableMultiLineString {
    #[wasm_bindgen(js_name = fromScalar)]
    pub fn from_scalar(value: MultiLineString) -> Self {
        Self(_BroadcastableMultiLineString::Scalar(value.into()))
    }

    #[wasm_bindgen(js_name = fromData)]
    pub fn from_data(values: MultiLineStringData) -> Self {
        Self(_BroadcastableMultiLineString::Array(values.0))
    }
}
