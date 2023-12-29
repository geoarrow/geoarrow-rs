use crate::data::LineStringData;
use crate::scalar::LineString;
use wasm_bindgen::prelude::*;

enum _BroadcastableLineString {
    Scalar(geoarrow::scalar::OwnedLineString<i32>),
    Array(geoarrow::array::LineStringArray<i32>),
}

#[wasm_bindgen]
pub struct BroadcastableLineString(_BroadcastableLineString);

#[wasm_bindgen]
impl BroadcastableLineString {
    #[wasm_bindgen(js_name = fromScalar)]
    pub fn from_scalar(value: LineString) -> Self {
        Self(_BroadcastableLineString::Scalar(value.into()))
    }

    #[wasm_bindgen(js_name = fromData)]
    pub fn from_data(values: LineStringData) -> Self {
        Self(_BroadcastableLineString::Array(values.0))
    }
}
