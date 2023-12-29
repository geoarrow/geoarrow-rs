use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub struct ChunkedRectArray(pub(crate) geoarrow::chunked_array::ChunkedRectArray);

impl From<geoarrow::chunked_array::ChunkedRectArray> for ChunkedRectArray {
    fn from(value: geoarrow::chunked_array::ChunkedRectArray) -> Self {
        Self(value)
    }
}

impl From<ChunkedRectArray> for geoarrow::chunked_array::ChunkedRectArray {
    fn from(value: ChunkedRectArray) -> Self {
        value.0
    }
}
