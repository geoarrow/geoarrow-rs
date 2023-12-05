use crate::error::WasmResult;
#[cfg(feature = "geodesy")]
use crate::reproject::ReprojectDirection;
use wasm_bindgen::prelude::*;

/// An immutable array of Rect geometries in WebAssembly memory using GeoArrow's in-memory
/// representation.
#[wasm_bindgen]
pub struct RectArray(pub(crate) geoarrow::array::RectArray);

impl From<geoarrow::array::RectArray> for RectArray {
    fn from(value: geoarrow::array::RectArray) -> Self {
        Self(value)
    }
}
