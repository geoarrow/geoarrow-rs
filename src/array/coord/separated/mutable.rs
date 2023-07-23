use crate::array::SeparatedCoordBuffer;

#[derive(Debug, Clone)]
pub struct MutableSeparatedCoordBuffer {
    x: Vec<f64>,
    y: Vec<f64>,
}

impl MutableSeparatedCoordBuffer {
    // TODO: switch this new (initializing to zero) to default?
    pub fn new() -> Self {
        Self::with_capacity(0)
    }

    pub fn from_vecs(x: Vec<f64>, y: Vec<f64>) -> Self {
        Self { x, y }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            x: Vec::with_capacity(capacity),
            y: Vec::with_capacity(capacity),
        }
    }

    /// Initialize a buffer of a given length with all coordinates set to 0.0
    pub fn initialize(len: usize) -> Self {
        Self {
            x: vec![0.0f64; len],
            y: vec![0.0f64; len],
        }
    }

    /// Returns the total number of coordinates the vector can hold without reallocating.
    pub fn capacity(&self) -> usize {
        self.x.capacity()
    }

    pub fn set_coord(&mut self, i: usize, coord: geo::Coord) {
        self.x[i] = coord.x;
        self.y[i] = coord.y;
    }

    pub fn push_coord(&mut self, coord: geo::Coord) {
        self.x.push(coord.x);
        self.y.push(coord.y);
    }

    pub fn set_xy(&mut self, i: usize, x: f64, y: f64) {
        self.x[i] = x;
        self.y[i] = y;
    }

    pub fn push_xy(&mut self, x: f64, y: f64) {
        self.x.push(x);
        self.y.push(y);
    }

    pub fn len(&self) -> usize {
        self.x.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl Default for MutableSeparatedCoordBuffer {
    fn default() -> Self {
        Self::with_capacity(0)
    }
}

impl From<MutableSeparatedCoordBuffer> for SeparatedCoordBuffer {
    fn from(value: MutableSeparatedCoordBuffer) -> Self {
        SeparatedCoordBuffer::new(value.x.into(), value.y.into())
    }
}
