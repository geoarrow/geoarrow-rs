use crate::geo_traits::CoordTrait;
use crate::scalar::InterleavedCoord;
use crate::trait_::GeometryScalarTrait;
use arrow2::buffer::Buffer;
use rstar::{RTreeObject, AABB};

#[derive(Debug, Clone, PartialEq)]
pub struct SeparatedCoord<'a> {
    pub x: &'a Buffer<f64>,
    pub y: &'a Buffer<f64>,
    pub i: usize,
}

impl<'a> GeometryScalarTrait<'a> for SeparatedCoord<'a> {
    type ScalarGeo = geo::Coord;

    fn to_geo(&self) -> Self::ScalarGeo {
        self.into()
    }
}

impl From<SeparatedCoord<'_>> for geo::Coord {
    fn from(value: SeparatedCoord) -> Self {
        (&value).into()
    }
}
impl From<&SeparatedCoord<'_>> for geo::Coord {
    fn from(value: &SeparatedCoord) -> Self {
        geo::Coord {
            x: value.x(),
            y: value.y(),
        }
    }
}

impl From<SeparatedCoord<'_>> for geo::Point {
    fn from(value: SeparatedCoord<'_>) -> Self {
        (&value).into()
    }
}

impl From<&SeparatedCoord<'_>> for geo::Point {
    fn from(value: &SeparatedCoord<'_>) -> Self {
        let coord: geo::Coord = value.into();
        coord.into()
    }
}

impl RTreeObject for SeparatedCoord<'_> {
    type Envelope = AABB<[f64; 2]>;

    fn envelope(&self) -> Self::Envelope {
        AABB::from_point([self.x(), self.y()])
    }
}

impl PartialEq<InterleavedCoord<'_>> for SeparatedCoord<'_> {
    fn eq(&self, other: &InterleavedCoord) -> bool {
        self.x_y() == other.x_y()
    }
}

impl CoordTrait for SeparatedCoord<'_> {
    type T = f64;

    fn x(&self) -> Self::T {
        self.x[self.i]
    }

    fn y(&self) -> Self::T {
        self.y[self.i]
    }
}

impl CoordTrait for &SeparatedCoord<'_> {
    type T = f64;

    fn x(&self) -> Self::T {
        self.x[self.i]
    }

    fn y(&self) -> Self::T {
        self.y[self.i]
    }
}
