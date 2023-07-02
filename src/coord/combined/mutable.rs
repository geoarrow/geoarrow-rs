use crate::{CoordBuffer, MutableInterleavedCoordBuffer, MutableSeparatedCoordBuffer};

#[derive(Debug, Clone)]
pub enum MutableCoordBuffer {
    Interleaved(MutableInterleavedCoordBuffer),
    Separated(MutableSeparatedCoordBuffer),
}

impl MutableCoordBuffer {
    pub fn push_coord(&mut self, coord: geo::Coord) {
        match self {
            MutableCoordBuffer::Interleaved(cb) => cb.push_coord(coord),
            MutableCoordBuffer::Separated(cb) => cb.push_coord(coord),
        }
    }

    pub fn len(&self) -> usize {
        match self {
            MutableCoordBuffer::Interleaved(cb) => cb.len(),
            MutableCoordBuffer::Separated(cb) => cb.len(),
        }
    }
}

impl From<MutableCoordBuffer> for CoordBuffer {
    fn from(value: MutableCoordBuffer) -> Self {
        match value {
            MutableCoordBuffer::Interleaved(cb) => CoordBuffer::Interleaved(cb.into()),
            MutableCoordBuffer::Separated(cb) => CoordBuffer::Separated(cb.into()),
        }
    }
}
