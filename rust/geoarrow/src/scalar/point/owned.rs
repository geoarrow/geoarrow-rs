use crate::algorithm::native::eq::point_eq;
use crate::array::{CoordBuffer, PointArray};
use crate::io::geo::point_to_geo;
use crate::scalar::{Coord, Point};
use geo_traits::PointTrait;

#[derive(Clone, Debug)]
pub struct OwnedPoint<const D: usize> {
    coords: CoordBuffer,
    geom_index: usize,
}

impl<const D: usize> OwnedPoint<D> {
    pub fn new(coords: CoordBuffer, geom_index: usize) -> Self {
        Self { coords, geom_index }
    }

    pub fn coord(&self) -> Coord {
        self.coords.value(self.geom_index)
    }
}

impl<'a, const D: usize> From<&'a OwnedPoint<D>> for Point<'a, D> {
    fn from(value: &'a OwnedPoint<D>) -> Self {
        Self::new(&value.coords, value.geom_index)
    }
}

impl<'a, const D: usize> From<Point<'a, D>> for OwnedPoint<D> {
    fn from(value: Point<'a, D>) -> Self {
        let (coords, geom_index) = value.into_owned_inner();
        Self::new(coords, geom_index)
    }
}

impl<const D: usize> From<OwnedPoint<D>> for PointArray<D> {
    fn from(value: OwnedPoint<D>) -> Self {
        Self::new(value.coords, None, Default::default())
    }
}

impl<const D: usize> PointTrait for OwnedPoint<D> {
    type T = f64;
    type CoordType<'a> = Coord<'a>;

    fn dim(&self) -> geo_traits::Dimensions {
        self.coords.dim().into()
    }

    fn coord(&self) -> Option<Self::CoordType<'_>> {
        let coord = self.coords.value(self.geom_index);
        if coord.is_nan() {
            None
        } else {
            Some(coord)
        }
    }
}

impl<const D: usize> From<OwnedPoint<D>> for geo::Point {
    fn from(value: OwnedPoint<D>) -> Self {
        (&value).into()
    }
}

impl<const D: usize> From<&OwnedPoint<D>> for geo::Point {
    fn from(value: &OwnedPoint<D>) -> Self {
        point_to_geo(value)
    }
}

impl<const D: usize> PartialEq for OwnedPoint<D> {
    fn eq(&self, other: &Self) -> bool {
        point_eq(self, other)
    }
}
