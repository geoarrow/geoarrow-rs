use geoarrow::array::GeometryArray;
use wasm_bindgen::prelude::*;

use crate::array::primitive::Float64Array;
use crate::error::WasmResult;

#[wasm_bindgen]
pub struct MultiLineStringArray(pub(crate) geoarrow::array::MultiLineStringArray);

#[wasm_bindgen]
impl MultiLineStringArray {
    #[wasm_bindgen]
    pub fn area(&self) -> WasmResult<Float64Array> {
        use geoarrow::algorithm::geo::area;
        let out = area(GeometryArray::MultiLineString(self.0.clone()))?;
        Ok(Float64Array(out))
    }
}
