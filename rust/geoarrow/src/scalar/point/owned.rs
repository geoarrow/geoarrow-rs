use crate::algorithm::native::eq::point_eq;
use crate::array::{CoordBuffer, PointArray};
use crate::scalar::{Coord, Point};
use geo_traits::PointTrait;
use geo_traits::to_geo::ToGeoPoint;

#[derive(Clone, Debug)]
pub struct OwnedPoint {
    coords: CoordBuffer,
    geom_index: usize,
}

impl OwnedPoint {
    pub fn new(coords: CoordBuffer, geom_index: usize) -> Self {
        Self { coords, geom_index }
    }

    pub fn coord(&self) -> Coord {
        self.coords.value(self.geom_index)
    }
}

impl<'a> From<&'a OwnedPoint> for Point<'a> {
    fn from(value: &'a OwnedPoint) -> Self {
        Self::new(&value.coords, value.geom_index)
    }
}

impl<'a> From<Point<'a>> for OwnedPoint {
    fn from(value: Point<'a>) -> Self {
        let (coords, geom_index) = value.into_owned_inner();
        Self::new(coords, geom_index)
    }
}

impl From<OwnedPoint> for PointArray {
    fn from(value: OwnedPoint) -> Self {
        Self::new(value.coords, None, Default::default())
    }
}

impl PointTrait for OwnedPoint {
    type T = f64;
    type CoordType<'a> = Coord<'a>;

    fn dim(&self) -> geo_traits::Dimensions {
        self.coords.dim().into()
    }

    fn coord(&self) -> Option<Self::CoordType<'_>> {
        let coord = self.coords.value(self.geom_index);
        if coord.is_nan() { None } else { Some(coord) }
    }
}

impl From<OwnedPoint> for geo::Point {
    fn from(value: OwnedPoint) -> Self {
        (&value).into()
    }
}

impl From<&OwnedPoint> for geo::Point {
    fn from(value: &OwnedPoint) -> Self {
        value.to_point()
    }
}

impl PartialEq for OwnedPoint {
    fn eq(&self, other: &Self) -> bool {
        point_eq(self, other)
    }
}
