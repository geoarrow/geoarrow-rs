use arrow2::buffer::Buffer;
use rstar::{RTreeObject, AABB};

pub struct SeparatedCoord<'a> {
    pub x: &'a Buffer<f64>,
    pub y: &'a Buffer<f64>,
    pub i: usize,
}

impl From<SeparatedCoord<'_>> for geo::Coord {
    fn from(value: SeparatedCoord) -> Self {
        geo::Coord {
            x: *value.x.get(value.i).unwrap(),
            y: *value.y.get(value.i).unwrap(),
        }
    }
}

impl From<SeparatedCoord<'_>> for geo::Point {
    fn from(value: SeparatedCoord<'_>) -> Self {
        let coord: geo::Coord = value.into();
        coord.into()
    }
}

impl RTreeObject for SeparatedCoord<'_> {
    type Envelope = AABB<[f64; 2]>;

    fn envelope(&self) -> Self::Envelope {
        AABB::from_point([self.x[self.i], self.y[self.i]])
    }
}
