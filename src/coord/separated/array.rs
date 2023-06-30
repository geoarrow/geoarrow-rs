use arrow_array::Float64Array;

#[derive(Debug, Clone)]
pub struct SeparatedCoordArray {
    x: Float64Array,
    y: Float64Array,
}

impl SeparatedCoordArray {
    pub fn new(x: Float64Array, y: Float64Array) -> Self {
        Self { x, y }
    }
}
