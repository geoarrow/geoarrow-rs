use crate::{InterleavedCoord, SeparatedCoord};

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
