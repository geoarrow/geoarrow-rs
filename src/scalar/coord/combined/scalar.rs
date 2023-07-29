use rstar::{RTreeObject, AABB};

use crate::geo_traits::CoordTrait;
use crate::scalar::{InterleavedCoord, SeparatedCoord};
use crate::trait_::GeometryScalarTrait;

#[derive(Debug, Clone, PartialEq)]
pub enum Coord<'a> {
    Separated(SeparatedCoord<'a>),
    Interleaved(InterleavedCoord<'a>),
}

impl<'a> GeometryScalarTrait<'a> for Coord<'a> {
    type ScalarGeo = geo::Coord;

    fn to_geo(&self) -> Self::ScalarGeo {
        self.into()
    }
}

impl From<Coord<'_>> for geo::Coord {
    fn from(value: Coord) -> Self {
        (&value).into()
    }
}

impl From<&Coord<'_>> for geo::Coord {
    fn from(value: &Coord) -> Self {
        match value {
            Coord::Separated(c) => c.into(),
            Coord::Interleaved(c) => c.into(),
        }
    }
}

impl From<Coord<'_>> for geo::Point {
    fn from(value: Coord) -> Self {
        (&value).into()
    }
}

impl From<&Coord<'_>> for geo::Point {
    fn from(value: &Coord) -> Self {
        match value {
            Coord::Separated(c) => c.into(),
            Coord::Interleaved(c) => c.into(),
        }
    }
}

impl RTreeObject for Coord<'_> {
    type Envelope = AABB<[f64; 2]>;

    fn envelope(&self) -> Self::Envelope {
        match self {
            Coord::Interleaved(c) => c.envelope(),
            Coord::Separated(c) => c.envelope(),
        }
    }
}

impl CoordTrait for Coord<'_> {
    type T = f64;

    fn x(&self) -> Self::T {
        match self {
            Coord::Interleaved(c) => c.x(),
            Coord::Separated(c) => c.x(),
        }
    }

    fn y(&self) -> Self::T {
        match self {
            Coord::Interleaved(c) => c.y(),
            Coord::Separated(c) => c.y(),
        }
    }
}

impl CoordTrait for &Coord<'_> {
    type T = f64;

    fn x(&self) -> Self::T {
        match self {
            Coord::Interleaved(c) => c.x(),
            Coord::Separated(c) => c.x(),
        }
    }

    fn y(&self) -> Self::T {
        match self {
            Coord::Interleaved(c) => c.y(),
            Coord::Separated(c) => c.y(),
        }
    }
}
