use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub struct ChunkedMultiPointArray(pub(crate) geoarrow::chunked_array::ChunkedMultiPointArray<i32>);

impl From<geoarrow::chunked_array::ChunkedMultiPointArray<i32>> for ChunkedMultiPointArray {
    fn from(value: geoarrow::chunked_array::ChunkedMultiPointArray<i32>) -> Self {
        Self(value)
    }
}

impl From<ChunkedMultiPointArray> for geoarrow::chunked_array::ChunkedMultiPointArray<i32> {
    fn from(value: ChunkedMultiPointArray) -> Self {
        value.0
    }
}
