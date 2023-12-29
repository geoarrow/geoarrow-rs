use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub struct ChunkedPointArray(pub(crate) geoarrow::chunked_array::ChunkedPointArray);

impl From<geoarrow::chunked_array::ChunkedPointArray> for ChunkedPointArray {
    fn from(value: geoarrow::chunked_array::ChunkedPointArray) -> Self {
        Self(value)
    }
}

impl From<ChunkedPointArray> for geoarrow::chunked_array::ChunkedPointArray {
    fn from(value: ChunkedPointArray) -> Self {
        value.0
    }
}
