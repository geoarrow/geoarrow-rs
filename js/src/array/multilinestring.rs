use crate::array::CoordBuffer;
use crate::error::WasmResult;
#[cfg(feature = "geodesy")]
use crate::reproject::ReprojectDirection;
use crate::utils::vec_to_offsets;
use wasm_bindgen::prelude::*;

/// An immutable array of MultiLineString geometries in WebAssembly memory using GeoArrow's
/// in-memory representation.
#[wasm_bindgen]
pub struct MultiLineStringArray(pub(crate) geoarrow::array::MultiLineStringArray<i32>);

#[wasm_bindgen]
impl MultiLineStringArray {
    #[wasm_bindgen(constructor)]
    pub fn new(coords: CoordBuffer, geom_offsets: Vec<i32>, ring_offsets: Vec<i32>) -> Self {
        Self(geoarrow::array::MultiLineStringArray::new(
            coords.0,
            vec_to_offsets(geom_offsets),
            vec_to_offsets(ring_offsets),
            None,
        ))
    }
}

impl From<geoarrow::array::MultiLineStringArray<i32>> for MultiLineStringArray {
    fn from(value: geoarrow::array::MultiLineStringArray<i32>) -> Self {
        Self(value)
    }
}
