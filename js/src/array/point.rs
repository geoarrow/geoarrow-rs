use geoarrow::array::GeometryArray;
use wasm_bindgen::prelude::*;

use crate::array::primitive::Float64Array;
use crate::error::WasmResult;

#[wasm_bindgen]
pub struct PointArray(pub(crate) geoarrow::array::PointArray);

#[wasm_bindgen]
impl PointArray {
    #[wasm_bindgen]
    pub fn area(&self) -> WasmResult<Float64Array> {
        use geoarrow::algorithm::geo::area;
        let out = area(GeometryArray::Point(self.0.clone()))?;
        Ok(Float64Array(out))
    }
}
