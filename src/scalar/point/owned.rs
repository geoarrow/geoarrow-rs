use crate::array::CoordBuffer;
use crate::scalar::Point;

pub struct OwnedPoint {
    coords: CoordBuffer,
    geom_index: usize,
}

impl OwnedPoint {
    pub fn new(coords: CoordBuffer, geom_index: usize) -> Self {
        Self { coords, geom_index }
    }
}

impl<'a> From<OwnedPoint> for Point<'a> {
    fn from(value: OwnedPoint) -> Self {
        Self::new_owned(value.coords, value.geom_index)
    }
}

impl From<OwnedPoint> for geo::Point {
    fn from(value: OwnedPoint) -> Self {
        let geom = Point::from(value);
        geom.into()
    }
}

impl<'a> From<Point<'a>> for OwnedPoint {
    fn from(value: Point<'a>) -> Self {
        let (coords, geom_index) = value.into_owned_inner();
        Self::new(coords, geom_index)
    }
}
