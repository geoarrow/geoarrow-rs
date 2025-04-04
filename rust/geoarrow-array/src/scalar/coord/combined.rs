use rstar::{RTreeObject, AABB};

use crate::scalar::{InterleavedCoord, SeparatedCoord};
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
