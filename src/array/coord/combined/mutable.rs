use crate::array::{CoordBuffer, MutableInterleavedCoordBuffer, MutableSeparatedCoordBuffer};

#[derive(Debug, Clone)]
pub enum MutableCoordBuffer {
    Interleaved(MutableInterleavedCoordBuffer),
    Separated(MutableSeparatedCoordBuffer),
}

impl MutableCoordBuffer {
    pub fn initialize(len: usize, interleaved: bool) -> Self {
        match interleaved {
            true => MutableCoordBuffer::Interleaved(MutableInterleavedCoordBuffer::initialize(len)),
            false => MutableCoordBuffer::Separated(MutableSeparatedCoordBuffer::initialize(len)),
        }
    }

    pub fn set_coord(&mut self, i: usize, coord: geo::Coord) {
        match self {
            MutableCoordBuffer::Interleaved(cb) => cb.set_coord(i, coord),
            MutableCoordBuffer::Separated(cb) => cb.set_coord(i, coord),
        }
    }

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

    pub fn is_empty(&self) -> bool {
        self.len() == 0
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
