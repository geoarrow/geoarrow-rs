use crate::data::PointData;
use crate::scalar::Point;
use wasm_bindgen::prelude::*;

enum _BroadcastablePoint {
    Scalar(geoarrow::scalar::OwnedPoint),
    Array(geoarrow::array::PointArray),
}

#[wasm_bindgen]
pub struct BroadcastablePoint(_BroadcastablePoint);

#[wasm_bindgen]
impl BroadcastablePoint {
    #[wasm_bindgen(js_name = fromScalar)]
    pub fn from_scalar(value: Point) -> Self {
        Self(_BroadcastablePoint::Scalar(value.into()))
    }

    #[wasm_bindgen(js_name = fromArray)]
    pub fn from_array(values: PointData) -> Self {
        Self(_BroadcastablePoint::Array(values.0))
    }
}
