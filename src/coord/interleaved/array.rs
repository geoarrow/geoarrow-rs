use arrow_array::Float64Array;

/// A an array of XY coordinates stored interleaved in a single buffer.
#[derive(Debug, Clone)]
pub struct InterleavedCoordArray {
    coords: Float64Array,
}

impl InterleavedCoordArray {
    pub fn new(coords: Float64Array) -> Self {
        Self { coords }
    }
}
