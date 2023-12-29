use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub struct ChunkedMixedGeometryArray(
    pub(crate) geoarrow::chunked_array::ChunkedMixedGeometryArray<i32>,
);

impl From<geoarrow::chunked_array::ChunkedMixedGeometryArray<i32>> for ChunkedMixedGeometryArray {
    fn from(value: geoarrow::chunked_array::ChunkedMixedGeometryArray<i32>) -> Self {
        Self(value)
    }
}

impl From<ChunkedMixedGeometryArray> for geoarrow::chunked_array::ChunkedMixedGeometryArray<i32> {
    fn from(value: ChunkedMixedGeometryArray) -> Self {
        value.0
    }
}
