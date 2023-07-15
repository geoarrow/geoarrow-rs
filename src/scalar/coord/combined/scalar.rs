use rstar::{RTreeObject, AABB};

use crate::scalar::{InterleavedCoord, SeparatedCoord};

pub enum Coord<'a> {
    Separated(SeparatedCoord<'a>),
    Interleaved(InterleavedCoord<'a>),
}

impl From<Coord<'_>> for geo::Coord {
    fn from(value: Coord) -> Self {
        match value {
            Coord::Separated(c) => c.into(),
            Coord::Interleaved(c) => c.into(),
        }
    }
}

impl From<Coord<'_>> for geo::Point {
    fn from(value: Coord) -> Self {
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
