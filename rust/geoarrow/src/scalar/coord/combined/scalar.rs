use rstar::{RTreeObject, AABB};

use crate::scalar::{InterleavedCoord, SeparatedCoord};
use crate::trait_::NativeScalar;
use geo_traits::to_geo::ToGeoCoord;
use geo_traits::CoordTrait;

/// An Arrow equivalent of a Coord
///
/// This implements [CoordTrait], which you can use to extract data.
#[derive(Debug, Clone)]
pub enum Coord<'a> {
    /// Separated coordinate
    Separated(SeparatedCoord<'a>),
    /// Interleaved coordinate
    Interleaved(InterleavedCoord<'a>),
}

impl Coord<'_> {
    /// Return `true` if all values in the coordinate are f64::NAN
    pub(crate) fn is_nan(&self) -> bool {
        match self {
            Coord::Separated(c) => c.is_nan(),
            Coord::Interleaved(c) => c.is_nan(),
        }
    }
}

impl NativeScalar for Coord<'_> {
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

impl From<Coord<'_>> for geo::Coord {
    fn from(value: Coord) -> Self {
        (&value).into()
    }
}

impl From<&Coord<'_>> for geo::Coord {
    fn from(value: &Coord) -> Self {
        value.to_coord()
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

impl PartialEq for Coord<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.x_y() == other.x_y()
    }
}

impl PartialEq<InterleavedCoord<'_>> for Coord<'_> {
    fn eq(&self, other: &InterleavedCoord<'_>) -> bool {
        self.x_y() == other.x_y()
    }
}

impl PartialEq<SeparatedCoord<'_>> for Coord<'_> {
    fn eq(&self, other: &SeparatedCoord<'_>) -> bool {
        self.x_y() == other.x_y()
    }
}

impl CoordTrait for Coord<'_> {
    type T = f64;

    fn dim(&self) -> geo_traits::Dimensions {
        match self {
            Coord::Interleaved(c) => c.dim(),
            Coord::Separated(c) => c.dim(),
        }
    }

    fn nth_or_panic(&self, n: usize) -> Self::T {
        match self {
            Coord::Interleaved(c) => c.nth_or_panic(n),
            Coord::Separated(c) => c.nth_or_panic(n),
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

impl CoordTrait for &Coord<'_> {
    type T = f64;

    fn dim(&self) -> geo_traits::Dimensions {
        match self {
            Coord::Interleaved(c) => c.dim(),
            Coord::Separated(c) => c.dim(),
        }
    }

    fn nth_or_panic(&self, n: usize) -> Self::T {
        match self {
            Coord::Interleaved(c) => c.nth_or_panic(n),
            Coord::Separated(c) => c.nth_or_panic(n),
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
