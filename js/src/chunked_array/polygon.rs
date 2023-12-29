use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub struct ChunkedPolygonArray(pub(crate) geoarrow::chunked_array::ChunkedPolygonArray<i32>);

impl From<geoarrow::chunked_array::ChunkedPolygonArray<i32>> for ChunkedPolygonArray {
    fn from(value: geoarrow::chunked_array::ChunkedPolygonArray<i32>) -> Self {
        Self(value)
    }
}

impl From<ChunkedPolygonArray> for geoarrow::chunked_array::ChunkedPolygonArray<i32> {
    fn from(value: ChunkedPolygonArray) -> Self {
        value.0
    }
}
