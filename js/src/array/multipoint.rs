use crate::array::CoordBuffer;
// use crate::error::WasmResult;
// #[cfg(feature = "geodesy")]
// use crate::reproject::ReprojectDirection;
use crate::utils::vec_to_offsets;
use wasm_bindgen::prelude::*;

/// An immutable array of MultiPoint geometries in WebAssembly memory using GeoArrow's in-memory
/// representation.
#[wasm_bindgen]
pub struct MultiPointArray(pub(crate) geoarrow::array::MultiPointArray<i32>);

#[wasm_bindgen]
impl MultiPointArray {
    #[wasm_bindgen(constructor)]
    pub fn new(coords: CoordBuffer, geom_offsets: Vec<i32>) -> Self {
        Self(geoarrow::array::MultiPointArray::new(
            coords.0,
            vec_to_offsets(geom_offsets),
            None,
        ))
    }
}

impl From<geoarrow::array::MultiPointArray<i32>> for MultiPointArray {
    fn from(value: geoarrow::array::MultiPointArray<i32>) -> Self {
        Self(value)
    }
}
