use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub struct InterleavedCoordBuffer(pub(crate) geoarrow::array::InterleavedCoordBuffer);

#[wasm_bindgen]
impl InterleavedCoordBuffer {
    #[wasm_bindgen(constructor)]
    pub fn new(coords: Vec<f64>) -> Self {
        Self(geoarrow::array::InterleavedCoordBuffer::new(coords.into()))
    }
}

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

#[wasm_bindgen]
pub struct CoordBuffer(pub(crate) geoarrow::array::CoordBuffer);

#[wasm_bindgen]
impl CoordBuffer {
    #[wasm_bindgen]
    pub fn from_interleaved_coords(coords: InterleavedCoordBuffer) -> Self {
        Self(geoarrow::array::CoordBuffer::Interleaved(coords.0))
    }

    #[wasm_bindgen]
    pub fn from_separated_coords(coords: SeparatedCoordBuffer) -> Self {
        Self(geoarrow::array::CoordBuffer::Separated(coords.0))
    }
}
