use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub struct ChunkedMultiLineStringArray(
    pub(crate) geoarrow::chunked_array::ChunkedMultiLineStringArray<i32>,
);

impl From<geoarrow::chunked_array::ChunkedMultiLineStringArray<i32>>
    for ChunkedMultiLineStringArray
{
    fn from(value: geoarrow::chunked_array::ChunkedMultiLineStringArray<i32>) -> Self {
        Self(value)
    }
}

impl From<ChunkedMultiLineStringArray>
    for geoarrow::chunked_array::ChunkedMultiLineStringArray<i32>
{
    fn from(value: ChunkedMultiLineStringArray) -> Self {
        value.0
    }
}
