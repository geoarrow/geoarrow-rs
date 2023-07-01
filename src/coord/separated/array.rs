use arrow2::buffer::Buffer;

#[derive(Debug, Clone)]
pub struct SeparatedCoordArray {
    x: Buffer<f64>,
    y: Buffer<f64>,
}

impl SeparatedCoordArray {
    pub fn new(x: Buffer<f64>, y: Buffer<f64>) -> Self {
        Self { x, y }
    }
}
