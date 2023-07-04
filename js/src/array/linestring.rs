use geoarrow::array::GeometryArray;
use wasm_bindgen::prelude::*;

use crate::array::primitive::Float64Array;
use crate::error::WasmResult;

#[wasm_bindgen]
pub struct LineStringArray(pub(crate) geoarrow::array::LineStringArray);

#[wasm_bindgen]
impl LineStringArray {
    #[wasm_bindgen]
    pub fn area(&self) -> WasmResult<Float64Array> {
        use geoarrow::algorithm::geo::area;
        let out = area(GeometryArray::LineString(self.0.clone()))?;
        Ok(Float64Array(out))
    }
}
