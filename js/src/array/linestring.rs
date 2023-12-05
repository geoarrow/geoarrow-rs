use crate::array::CoordBuffer;
use crate::error::WasmResult;
#[cfg(feature = "geodesy")]
use crate::reproject::ReprojectDirection;
use crate::utils::vec_to_offsets;
use wasm_bindgen::prelude::*;

/// An immutable array of LineString geometries in WebAssembly memory using GeoArrow's in-memory
/// representation.
#[wasm_bindgen]
pub struct LineStringArray(pub(crate) geoarrow::array::LineStringArray<i32>);

#[wasm_bindgen]
impl LineStringArray {
    #[wasm_bindgen(constructor)]
    pub fn new(coords: CoordBuffer, geom_offsets: Vec<i32>) -> Self {
        Self(geoarrow::array::LineStringArray::new(
            coords.0,
            vec_to_offsets(geom_offsets),
            None,
        ))
    }
}

impl From<geoarrow::array::LineStringArray<i32>> for LineStringArray {
    fn from(value: geoarrow::array::LineStringArray<i32>) -> Self {
        Self(value)
    }
}
