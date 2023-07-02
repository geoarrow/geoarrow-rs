use crate::SeparatedCoordBuffer;

#[derive(Debug, Clone)]
pub struct MutableSeparatedCoordBuffer {
    x: Vec<f64>,
    y: Vec<f64>,
}

impl MutableSeparatedCoordBuffer {
    pub fn new() -> Self {
        Self::with_capacity(0)
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            x: Vec::with_capacity(capacity),
            y: Vec::with_capacity(capacity),
        }
    }

    pub fn push_coord(&mut self, coord: geo::Coord) {
        self.x.push(coord.x);
        self.y.push(coord.y);
    }

    pub fn len(&self) -> usize {
        self.x.len()
    }
}

impl From<MutableSeparatedCoordBuffer> for SeparatedCoordBuffer {
    fn from(value: MutableSeparatedCoordBuffer) -> Self {
        SeparatedCoordBuffer::new(value.x.into(), value.y.into())
    }
}
