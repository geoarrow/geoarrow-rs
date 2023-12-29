use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub struct ChunkedWKBArray(pub(crate) geoarrow::chunked_array::ChunkedWKBArray<i32>);

impl From<geoarrow::chunked_array::ChunkedWKBArray<i32>> for ChunkedWKBArray {
    fn from(value: geoarrow::chunked_array::ChunkedWKBArray<i32>) -> Self {
        Self(value)
    }
}

impl From<ChunkedWKBArray> for geoarrow::chunked_array::ChunkedWKBArray<i32> {
    fn from(value: ChunkedWKBArray) -> Self {
        value.0
    }
}
