use crate::data::PolygonData;
use crate::scalar::Polygon;
use wasm_bindgen::prelude::*;

enum _BroadcastablePolygon {
    Scalar(geoarrow::scalar::OwnedPolygon<i32>),
    Array(geoarrow::array::PolygonArray<i32>),
}

#[wasm_bindgen]
pub struct BroadcastablePolygon(_BroadcastablePolygon);

#[wasm_bindgen]
impl BroadcastablePolygon {
    #[wasm_bindgen(js_name = fromScalar)]
    pub fn from_scalar(value: Polygon) -> Self {
        Self(_BroadcastablePolygon::Scalar(value.into()))
    }

    #[wasm_bindgen(js_name = fromData)]
    pub fn from_data(values: PolygonData) -> Self {
        Self(_BroadcastablePolygon::Array(values.0))
    }
}
