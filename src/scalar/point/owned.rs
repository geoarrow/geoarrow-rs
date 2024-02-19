use crate::algorithm::native::eq::point_eq;
use crate::array::CoordBuffer;
use crate::geo_traits::{CoordTrait, PointTrait};
use crate::io::geo::point_to_geo;
use crate::scalar::Point;

#[derive(Debug)]
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

impl<'a> From<&'a OwnedPoint> for Point<'a> {
    fn from(value: &'a OwnedPoint) -> Self {
        Self::new_borrowed(&value.coords, value.geom_index)
    }
}

impl<'a> From<Point<'a>> for OwnedPoint {
    fn from(value: Point<'a>) -> Self {
        let (coords, geom_index) = value.into_owned_inner();
        Self::new(coords, geom_index)
    }
}

impl PointTrait for OwnedPoint {
    type T = f64;

    fn x(&self) -> f64 {
        self.coords.get_x(self.geom_index)
    }

    fn y(&self) -> f64 {
        self.coords.get_y(self.geom_index)
    }
}

impl CoordTrait for OwnedPoint {
    type T = f64;

    fn x(&self) -> Self::T {
        self.coords.get_x(self.geom_index)
    }

    fn y(&self) -> Self::T {
        self.coords.get_y(self.geom_index)
    }
}

impl From<OwnedPoint> for geo::Point {
    fn from(value: OwnedPoint) -> Self {
        value.into()
    }
}

impl From<&OwnedPoint> for geo::Point {
    fn from(value: &OwnedPoint) -> Self {
        point_to_geo(value)
    }
}

impl PartialEq for OwnedPoint {
    fn eq(&self, other: &Self) -> bool {
        point_eq(self, other, true)
    }
}
