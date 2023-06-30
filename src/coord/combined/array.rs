use crate::{InterleavedCoordArray, SeparatedCoordArray};

#[derive(Debug, Clone)]
pub enum CoordArray {
    Interleaved(InterleavedCoordArray),
    Separated(SeparatedCoordArray),
}
