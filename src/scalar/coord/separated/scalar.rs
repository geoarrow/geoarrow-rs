use crate::algorithm::native::eq::coord_eq;
use crate::geo_traits::CoordTrait;
use crate::scalar::InterleavedCoord;
use crate::trait_::GeometryScalarTrait;
use arrow_buffer::ScalarBuffer;
use rstar::{RTreeObject, AABB};

#[derive(Debug, Clone)]
pub struct SeparatedCoord<'a> {
    pub x: &'a ScalarBuffer<f64>,
    pub y: &'a ScalarBuffer<f64>,
    pub i: usize,
}

impl<'a> GeometryScalarTrait for SeparatedCoord<'a> {
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

impl PartialEq for SeparatedCoord<'_> {
    fn eq(&self, other: &SeparatedCoord) -> bool {
        coord_eq(self, other)
    }
}

impl PartialEq<InterleavedCoord<'_>> for SeparatedCoord<'_> {
    fn eq(&self, other: &InterleavedCoord) -> bool {
        coord_eq(self, other)
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

#[cfg(test)]
mod test {
    use crate::array::{InterleavedCoordBuffer, SeparatedCoordBuffer};
    use crate::trait_::GeometryArrayAccessor;

    /// Test Eq where the current index is true but another index is false
    #[test]
    fn test_eq_other_index_false() {
        let x1 = vec![0., 1., 2.];
        let y1 = vec![3., 4., 5.];
        let buf1 = SeparatedCoordBuffer::new(x1.into(), y1.into());
        let coord1 = buf1.value(0);

        let x2 = vec![0., 100., 2.];
        let y2 = vec![3., 400., 5.];
        let buf2 = SeparatedCoordBuffer::new(x2.into(), y2.into());
        let coord2 = buf2.value(0);

        assert_eq!(coord1, coord2);
    }

    #[test]
    fn test_eq_against_interleaved_coord() {
        let x1 = vec![0., 1., 2.];
        let y1 = vec![3., 4., 5.];
        let buf1 = SeparatedCoordBuffer::new(x1.into(), y1.into());
        let coord1 = buf1.value(0);

        let coords2 = vec![0., 3., 1., 4., 2., 5.];
        let buf2 = InterleavedCoordBuffer::new(coords2.into());
        let coord2 = buf2.value(0);

        assert_eq!(coord1, coord2);
    }
}
