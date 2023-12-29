use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub struct ChunkedGeometryCollectionArray(
    pub(crate) geoarrow::chunked_array::ChunkedGeometryCollectionArray<i32>,
);

impl From<geoarrow::chunked_array::ChunkedGeometryCollectionArray<i32>>
    for ChunkedGeometryCollectionArray
{
    fn from(value: geoarrow::chunked_array::ChunkedGeometryCollectionArray<i32>) -> Self {
        Self(value)
    }
}

impl From<ChunkedGeometryCollectionArray>
    for geoarrow::chunked_array::ChunkedGeometryCollectionArray<i32>
{
    fn from(value: ChunkedGeometryCollectionArray) -> Self {
        value.0
    }
}
