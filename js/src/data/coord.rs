use geoarrow_array::array::{CoordBuffer, InterleavedCoordBuffer, SeparatedCoordBuffer};
use geoarrow_schema::Dimension;
use wasm_bindgen::prelude::*;

use crate::dimension::JsDimension;

/// An immutable buffer of coordinates in WebAssembly memory, that can be either interleaved or
/// separated.
#[wasm_bindgen]
#[allow(dead_code)]
pub struct JsCoordBuffer(CoordBuffer);

#[wasm_bindgen]
impl JsCoordBuffer {
    /// Create a new CoordBuffer from a `Float64Array` of interleaved XY coordinates
    #[wasm_bindgen(js_name = fromInterleaved)]
    pub fn from_interleaved(coords: Vec<f64>, dim: JsDimension) -> Self {
        Self(InterleavedCoordBuffer::new(coords.into(), dim.into()).into())
    }

    /// Create a new CoordBuffer from two `Float64Array`s of X and Y
    #[wasm_bindgen(js_name = fromSeparated)]
    pub fn from_separated(x: Vec<f64>, y: Vec<f64>) -> Self {
        let cb = SeparatedCoordBuffer::from_vec(vec![x.into(), y.into()], Dimension::XY).unwrap();
        Self(cb.into())
    }
}
