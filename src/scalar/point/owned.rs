use crate::algorithm::native::eq::point_eq;
use crate::array::{CoordBuffer, PointArray};
use crate::geo_traits::{CoordTrait, PointTrait};
use crate::io::geo::point_to_geo;
use crate::scalar::{Coord, Point};
use crate::trait_::NativeArrayAccessor;

#[derive(Clone, Debug)]
pub struct OwnedPoint<const D: usize> {
    coords: CoordBuffer<D>,
    geom_index: usize,
}

impl<const D: usize> OwnedPoint<D> {
    pub fn new(coords: CoordBuffer<D>, geom_index: usize) -> Self {
        Self { coords, geom_index }
    }

    pub fn coord(&self) -> Coord<D> {
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

    fn dim(&self) -> usize {
        D
    }

    fn nth_unchecked(&self, n: usize) -> Self::T {
        let coord = self.coords.value(self.geom_index);
        CoordTrait::nth_unchecked(&coord, n)
    }

    fn x(&self) -> f64 {
        self.coords.get_x(self.geom_index)
    }

    fn y(&self) -> f64 {
        self.coords.get_y(self.geom_index)
    }
}

impl<const D: usize> CoordTrait for OwnedPoint<D> {
    type T = f64;

    fn dim(&self) -> usize {
        D
    }

    fn nth_unchecked(&self, n: usize) -> Self::T {
        let coord = self.coords.value(self.geom_index);
        CoordTrait::nth_unchecked(&coord, n)
    }

    fn x(&self) -> Self::T {
        self.coords.get_x(self.geom_index)
    }

    fn y(&self) -> Self::T {
        self.coords.get_y(self.geom_index)
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
        point_eq(self, other, true)
    }
}
