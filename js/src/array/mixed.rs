use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub struct MixedGeometryArray(pub(crate) geoarrow::array::MixedGeometryArray<i32>);

impl From<MixedGeometryArray> for geoarrow::array::MixedGeometryArray<i32> {
    fn from(value: MixedGeometryArray) -> Self {
        value.0
    }
}

impl From<geoarrow::array::MixedGeometryArray<i32>> for MixedGeometryArray {
    fn from(value: geoarrow::array::MixedGeometryArray<i32>) -> Self {
        Self(value)
    }
}
