use crate::{InterleavedCoordArray, SeparatedCoordArray, GeometryArrayTrait};

#[derive(Debug, Clone)]
pub enum CoordArray {
    Interleaved(InterleavedCoordArray),
    Separated(SeparatedCoordArray),
}

impl CoordArray {
    pub fn get_x(&self, i: usize) -> f64 {
        // NOTE: for interleaved this needs to be i*2 so it accesses the right point
        todo!();
    }

    pub fn get_y(&self, i: usize) -> f64 {
        todo!();
    }

    pub fn len(&self) -> usize {
        todo!()
    }
}

// impl GeometryArrayTrait for CoordArray {

// }
