use crate::array::CoordBuffer;
use crate::error::WasmResult;
use crate::impl_geometry_array;
#[cfg(feature = "geodesy")]
use crate::reproject::ReprojectDirection;
use wasm_bindgen::prelude::*;

/// An immutable array of Point geometries in WebAssembly memory using GeoArrow's in-memory
/// representation.
#[wasm_bindgen]
pub struct PointArray(pub(crate) geoarrow::array::PointArray);

impl_geometry_array!(PointArray);

#[wasm_bindgen]
impl PointArray {
    #[wasm_bindgen(constructor)]
    pub fn new(coords: CoordBuffer) -> Self {
        Self(geoarrow::array::PointArray::new(coords.0, None))
    }
}

impl From<geoarrow::array::PointArray> for PointArray {
    fn from(value: geoarrow::array::PointArray) -> Self {
        Self(value)
    }
}
