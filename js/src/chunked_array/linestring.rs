use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub struct ChunkedLineStringArray(pub(crate) geoarrow::chunked_array::ChunkedLineStringArray<i32>);

impl From<geoarrow::chunked_array::ChunkedLineStringArray<i32>> for ChunkedLineStringArray {
    fn from(value: geoarrow::chunked_array::ChunkedLineStringArray<i32>) -> Self {
        Self(value)
    }
}

impl From<ChunkedLineStringArray> for geoarrow::chunked_array::ChunkedLineStringArray<i32> {
    fn from(value: ChunkedLineStringArray) -> Self {
        value.0
    }
}
