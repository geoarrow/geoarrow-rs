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

    pub fn push_coord(&mut self, coord: geo::Coord) {
        self.coords.push(coord.x);
        self.coords.push(coord.y);
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
