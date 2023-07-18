use wasm_bindgen::prelude::*;

// TODO: remove InterleavedCoordBuffer and SeparatedCoordBuffer structs?

/// An immutable buffer of interleaved coordinates in WebAssembly memory.
#[wasm_bindgen]
pub struct InterleavedCoordBuffer(pub(crate) geoarrow::array::InterleavedCoordBuffer);

#[wasm_bindgen]
impl InterleavedCoordBuffer {
    #[wasm_bindgen(constructor)]
    pub fn new(coords: Vec<f64>) -> Self {
        Self(geoarrow::array::InterleavedCoordBuffer::new(coords.into()))
    }
}

/// An immutable buffer of separated coordinates in WebAssembly memory.
#[wasm_bindgen]
pub struct SeparatedCoordBuffer(pub(crate) geoarrow::array::SeparatedCoordBuffer);

#[wasm_bindgen]
impl SeparatedCoordBuffer {
    #[wasm_bindgen(constructor)]
    pub fn new(x: Vec<f64>, y: Vec<f64>) -> Self {
        Self(geoarrow::array::SeparatedCoordBuffer::new(
            x.into(),
            y.into(),
        ))
    }
}

/// An immutable buffer of coordinates in WebAssembly memory, that can be either interleaved or
/// separated.
#[wasm_bindgen]
pub struct CoordBuffer(pub(crate) geoarrow::array::CoordBuffer);

#[wasm_bindgen]
impl CoordBuffer {
    /// Create a new CoordBuffer from a `Float64Array` of interleaved XY coordinates
    #[wasm_bindgen(js_name = fromInterleaved)]
    pub fn from_interleaved(coords: Vec<f64>) -> Self {
        let buffer = geoarrow::array::InterleavedCoordBuffer::new(coords.into());
        Self(geoarrow::array::CoordBuffer::Interleaved(buffer))
    }

    /// Create a new CoordBuffer from two `Float64Array`s of X and Y
    #[wasm_bindgen(js_name = fromSeparated)]
    pub fn from_separated(x: Vec<f64>, y: Vec<f64>) -> Self {
        let buffer = geoarrow::array::SeparatedCoordBuffer::new(x.into(), y.into());
        Self(geoarrow::array::CoordBuffer::Separated(buffer))
    }

    /// Create a new CoordBuffer from an `InterleavedCoordBuffer` object
    #[wasm_bindgen(js_name = fromInterleavedCoords)]
    pub fn from_interleaved_coords(coords: InterleavedCoordBuffer) -> Self {
        Self(geoarrow::array::CoordBuffer::Interleaved(coords.0))
    }

    /// Create a new CoordBuffer from a `SeparatedCoordBuffer` object
    #[wasm_bindgen(js_name = fromSeparatedCoords)]
    pub fn from_separated_coords(coords: SeparatedCoordBuffer) -> Self {
        Self(geoarrow::array::CoordBuffer::Separated(coords.0))
    }
}
