use crate::array::MultiPointArray;
use crate::scalar::MultiPoint;
use wasm_bindgen::prelude::*;

enum _BroadcastableMultiPoint {
    Scalar(geoarrow::scalar::OwnedMultiPoint<i32>),
    Array(geoarrow::array::MultiPointArray<i32>),
}

#[wasm_bindgen]
pub struct BroadcastableMultiPoint(_BroadcastableMultiPoint);

#[wasm_bindgen]
impl BroadcastableMultiPoint {
    #[wasm_bindgen(js_name = fromScalar)]
    pub fn from_scalar(value: MultiPoint) -> Self {
        Self(_BroadcastableMultiPoint::Scalar(value.into()))
    }

    #[wasm_bindgen(js_name = fromArray)]
    pub fn from_array(values: MultiPointArray) -> Self {
        Self(_BroadcastableMultiPoint::Array(values.0))
    }
}
