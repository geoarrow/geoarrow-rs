use crate::data::MultiPointData;
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

    #[wasm_bindgen(js_name = fromData)]
    pub fn from_data(values: MultiPointData) -> Self {
        Self(_BroadcastableMultiPoint::Array(values.0))
    }
}
