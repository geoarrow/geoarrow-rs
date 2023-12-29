use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub struct ChunkedMultiPolygonArray(
    pub(crate) geoarrow::chunked_array::ChunkedMultiPolygonArray<i32>,
);

impl From<geoarrow::chunked_array::ChunkedMultiPolygonArray<i32>> for ChunkedMultiPolygonArray {
    fn from(value: geoarrow::chunked_array::ChunkedMultiPolygonArray<i32>) -> Self {
        Self(value)
    }
}

impl From<ChunkedMultiPolygonArray> for geoarrow::chunked_array::ChunkedMultiPolygonArray<i32> {
    fn from(value: ChunkedMultiPolygonArray) -> Self {
        value.0
    }
}
