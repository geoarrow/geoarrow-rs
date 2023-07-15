use arrow2::buffer::Buffer;
use rstar::{RTreeObject, AABB};

pub struct InterleavedCoord<'a> {
    pub coords: &'a Buffer<f64>,
    pub i: usize,
}

impl From<InterleavedCoord<'_>> for geo::Coord {
    fn from(value: InterleavedCoord) -> Self {
        geo::Coord {
            x: *value.coords.get(value.i * 2).unwrap(),
            y: *value.coords.get(value.i * 2 + 1).unwrap(),
        }
    }
}

impl From<InterleavedCoord<'_>> for geo::Point {
    fn from(value: InterleavedCoord<'_>) -> Self {
        let coord: geo::Coord = value.into();
        coord.into()
    }
}

impl RTreeObject for InterleavedCoord<'_> {
    type Envelope = AABB<[f64; 2]>;

    fn envelope(&self) -> Self::Envelope {
        AABB::from_point([self.coords[self.i * 2], self.coords[self.i * 2 + 1]])
    }
}
