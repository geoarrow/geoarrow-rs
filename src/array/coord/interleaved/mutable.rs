use crate::array::InterleavedCoordBuffer;

#[derive(Debug, Clone)]
pub struct MutableInterleavedCoordBuffer {
    pub coords: Vec<f64>,
}

impl MutableInterleavedCoordBuffer {
    pub fn new() -> Self {
        Self::with_capacity(0)
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            coords: Vec::with_capacity(capacity * 2),
        }
    }

    /// Initialize a buffer of a given length with all coordinates set to 0.0
    pub fn initialize(len: usize) -> Self {
        Self {
            coords: vec![0.0f64; len * 2],
        }
    }

    pub fn set_coord(&mut self, i: usize, coord: geo::Coord) {
        self.coords[i * 2] = coord.x;
        self.coords[i * 2 + 1] = coord.y;
    }

    pub fn push_coord(&mut self, coord: geo::Coord) {
        self.coords.push(coord.x);
        self.coords.push(coord.y);
    }

    pub fn push_xy(&mut self, x: f64, y: f64) {
        self.coords.push(x);
        self.coords.push(y);
    }

    pub fn len(&self) -> usize {
        self.coords.len() / 2
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl Default for MutableInterleavedCoordBuffer {
    fn default() -> Self {
        Self::with_capacity(0)
    }
}

impl From<MutableInterleavedCoordBuffer> for InterleavedCoordBuffer {
    fn from(value: MutableInterleavedCoordBuffer) -> Self {
        InterleavedCoordBuffer::new(value.coords.into())
    }
}
