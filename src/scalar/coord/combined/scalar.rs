use rstar::{RTreeObject, AABB};

use crate::geo_traits::CoordTrait;
use crate::io::geo::coord_to_geo;
use crate::scalar::{InterleavedCoord, SeparatedCoord};
use crate::trait_::NativeScalar;

#[derive(Debug, Clone)]
pub enum Coord<'a, const D: usize> {
    Separated(SeparatedCoord<'a, D>),
    Interleaved(InterleavedCoord<'a, D>),
}

impl<'a, const D: usize> NativeScalar for Coord<'a, D> {
    type ScalarGeo = geo::Coord;

    fn to_geo(&self) -> Self::ScalarGeo {
        self.into()
    }

    fn to_geo_geometry(&self) -> geo::Geometry {
        todo!()
    }

    #[cfg(feature = "geos")]
    fn to_geos(&self) -> std::result::Result<geos::Geometry, geos::Error> {
        todo!()
        // self.try_into()
    }
}

impl<const D: usize> From<Coord<'_, D>> for geo::Coord {
    fn from(value: Coord<D>) -> Self {
        (&value).into()
    }
}

impl<const D: usize> From<&Coord<'_, D>> for geo::Coord {
    fn from(value: &Coord<D>) -> Self {
        coord_to_geo(value)
    }
}

impl<const D: usize> From<Coord<'_, D>> for geo::Point {
    fn from(value: Coord<D>) -> Self {
        (&value).into()
    }
}

impl<const D: usize> From<&Coord<'_, D>> for geo::Point {
    fn from(value: &Coord<D>) -> Self {
        match value {
            Coord::Separated(c) => c.into(),
            Coord::Interleaved(c) => c.into(),
        }
    }
}

impl<const D: usize> RTreeObject for Coord<'_, D> {
    type Envelope = AABB<[f64; 2]>;

    fn envelope(&self) -> Self::Envelope {
        match self {
            Coord::Interleaved(c) => c.envelope(),
            Coord::Separated(c) => c.envelope(),
        }
    }
}

impl<const D: usize> PartialEq for Coord<'_, D> {
    fn eq(&self, other: &Self) -> bool {
        self.x_y() == other.x_y()
    }
}

impl<const D: usize> PartialEq<InterleavedCoord<'_, D>> for Coord<'_, D> {
    fn eq(&self, other: &InterleavedCoord<'_, D>) -> bool {
        self.x_y() == other.x_y()
    }
}

impl<const D: usize> PartialEq<SeparatedCoord<'_, D>> for Coord<'_, D> {
    fn eq(&self, other: &SeparatedCoord<'_, D>) -> bool {
        self.x_y() == other.x_y()
    }
}

impl<const D: usize> CoordTrait for Coord<'_, D> {
    type T = f64;

    fn dim(&self) -> usize {
        D
    }

    fn nth_unchecked(&self, n: usize) -> Self::T {
        match self {
            Coord::Interleaved(c) => c.nth_unchecked(n),
            Coord::Separated(c) => c.nth_unchecked(n),
        }
    }

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

impl<const D: usize> CoordTrait for &Coord<'_, D> {
    type T = f64;

    fn dim(&self) -> usize {
        D
    }

    fn nth_unchecked(&self, n: usize) -> Self::T {
        match self {
            Coord::Interleaved(c) => c.nth_unchecked(n),
            Coord::Separated(c) => c.nth_unchecked(n),
        }
    }

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
