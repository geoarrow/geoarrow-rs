use std::borrow::Cow;

use geos::CoordSeq;

use crate::geo_traits::{CoordTrait, PointTrait};

#[derive(Clone)]
pub struct GEOSCoord<'a> {
    /// The underlying coordinate sequence
    coord_seq: Cow<'a, CoordSeq<'a>>,

    /// The offset into the buffer where this coordinate is located
    ///
    /// Note that this does not have to be immediately after the WKB header! For a `WKBPoint`, the
    /// `Point` is immediately after the header, but the `Point` also appears in other geometry
    /// types. I.e. the `WKBLineString` has a header, then the number of points, then a sequence of
    /// `Point` objects.
    offset: usize,
}

impl<'a> GEOSCoord<'a> {
    pub fn new_owned(coord_seq: CoordSeq<'a>, offset: usize) -> Self {
        Self {
            coord_seq: Cow::Owned(coord_seq),
            offset,
        }
    }

    pub fn new_borrowed(coord_seq: &'a CoordSeq<'a>, offset: usize) -> Self {
        Self {
            coord_seq: Cow::Borrowed(coord_seq),
            offset,
        }
    }
}

impl<'a> CoordTrait for GEOSCoord<'a> {
    type T = f64;

    fn x(&self) -> Self::T {
        self.coord_seq.get_x(self.offset).unwrap()
    }

    fn y(&self) -> Self::T {
        self.coord_seq.get_y(self.offset).unwrap()
    }
}

impl<'a> PointTrait for GEOSCoord<'a> {
    type T = f64;

    fn x(&self) -> Self::T {
        self.coord_seq.get_x(self.offset).unwrap()
    }

    fn y(&self) -> Self::T {
        self.coord_seq.get_y(self.offset).unwrap()
    }
}
