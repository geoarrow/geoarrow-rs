use crate::SeparatedCoordBuffer;

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

    pub fn push_coord(&mut self, coord: geo::Coord) {
        self.x.push(coord.x);
        self.y.push(coord.y);
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
