use crate::CoordArray;

pub struct Point<'a> {
    coords: &'a CoordArray,
    geom_index: usize,
}

impl<'a> Point<'a> {
    pub fn new(coords: &'a CoordArray, geom_index: usize) -> Self {
        Self { coords, geom_index }
    }
}
