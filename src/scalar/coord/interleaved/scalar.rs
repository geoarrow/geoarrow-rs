use arrow2::buffer::Buffer;
use rstar::{RTreeObject, AABB};

use crate::geo_traits::CoordTrait;
use crate::scalar::SeparatedCoord;
use crate::trait_::GeometryScalarTrait;

#[derive(Debug, Clone, PartialEq)]
pub struct InterleavedCoord<'a> {
    pub coords: &'a Buffer<f64>,
    pub i: usize,
}

impl<'a> GeometryScalarTrait<'a> for InterleavedCoord<'a> {
    type ScalarGeo = geo::Coord;

    fn to_geo(&self) -> Self::ScalarGeo {
        self.into()
    }
}

impl From<InterleavedCoord<'_>> for geo::Coord {
    fn from(value: InterleavedCoord) -> Self {
        (&value).into()
    }
}

impl From<&InterleavedCoord<'_>> for geo::Coord {
    fn from(value: &InterleavedCoord) -> Self {
        geo::Coord {
            x: value.x(),
            y: value.y(),
        }
    }
}

impl From<InterleavedCoord<'_>> for geo::Point {
    fn from(value: InterleavedCoord<'_>) -> Self {
        (&value).into()
    }
}

impl From<&InterleavedCoord<'_>> for geo::Point {
    fn from(value: &InterleavedCoord<'_>) -> Self {
        let coord: geo::Coord = value.into();
        coord.into()
    }
}

impl RTreeObject for InterleavedCoord<'_> {
    type Envelope = AABB<[f64; 2]>;

    fn envelope(&self) -> Self::Envelope {
        AABB::from_point([self.x(), self.y()])
    }
}

impl PartialEq<SeparatedCoord<'_>> for InterleavedCoord<'_> {
    fn eq(&self, other: &SeparatedCoord<'_>) -> bool {
        self.x_y() == other.x_y()
    }
}

impl CoordTrait for InterleavedCoord<'_> {
    type T = f64;

    fn x(&self) -> Self::T {
        *self.coords.get(self.i * 2).unwrap()
    }

    fn y(&self) -> Self::T {
        *self.coords.get(self.i * 2 + 1).unwrap()
    }
}

impl CoordTrait for &InterleavedCoord<'_> {
    type T = f64;

    fn x(&self) -> Self::T {
        *self.coords.get(self.i * 2).unwrap()
    }

    fn y(&self) -> Self::T {
        *self.coords.get(self.i * 2 + 1).unwrap()
    }
}
