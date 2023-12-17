use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub struct GeometryCollectionArray(pub(crate) geoarrow::array::GeometryCollectionArray<i32>);

impl From<GeometryCollectionArray> for geoarrow::array::GeometryCollectionArray<i32> {
    fn from(value: GeometryCollectionArray) -> Self {
        value.0
    }
}

impl From<geoarrow::array::GeometryCollectionArray<i32>> for GeometryCollectionArray {
    fn from(value: geoarrow::array::GeometryCollectionArray<i32>) -> Self {
        Self(value)
    }
}
