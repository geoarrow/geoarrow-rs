use arrow_array::types::{Float64Type, UInt32Type};
use geoarrow::algorithm::broadcasting::BroadcastablePrimitive;
use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub struct BroadcastableFloat(pub(crate) BroadcastablePrimitive<Float64Type>);

#[wasm_bindgen]
impl BroadcastableFloat {
    #[wasm_bindgen(js_name = fromScalar)]
    pub fn from_scalar(value: f64) -> Self {
        Self(BroadcastablePrimitive::Scalar(value))
    }

    #[wasm_bindgen(js_name = fromArray)]
    pub fn from_array(values: Vec<f64>) -> Self {
        Self(BroadcastablePrimitive::Array(values.into()))
    }
}

#[wasm_bindgen]
pub struct BroadcastableUint32(pub(crate) BroadcastablePrimitive<UInt32Type>);

#[wasm_bindgen]
impl BroadcastableUint32 {
    #[wasm_bindgen(js_name = fromScalar)]
    pub fn from_scalar(value: u32) -> Self {
        Self(BroadcastablePrimitive::Scalar(value))
    }

    #[wasm_bindgen(js_name = fromArray)]
    pub fn from_array(values: Vec<u32>) -> Self {
        Self(BroadcastablePrimitive::Array(values.into()))
    }
}
