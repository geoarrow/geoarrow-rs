use arrow_buffer::ScalarBuffer;
use rstar::{RTreeObject, AABB};

use crate::algorithm::native::eq::coord_eq;
use crate::geo_traits::CoordTrait;
use crate::scalar::SeparatedCoord;
use crate::trait_::GeometryScalarTrait;

#[derive(Debug, Clone)]
pub struct InterleavedCoord<'a> {
    pub coords: &'a ScalarBuffer<f64>,
    pub i: usize,
}

impl<'a> GeometryScalarTrait for InterleavedCoord<'a> {
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

impl PartialEq for InterleavedCoord<'_> {
    fn eq(&self, other: &Self) -> bool {
        coord_eq(self, other)
    }
}

impl PartialEq<SeparatedCoord<'_>> for InterleavedCoord<'_> {
    fn eq(&self, other: &SeparatedCoord<'_>) -> bool {
        coord_eq(self, other)
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

#[cfg(test)]
mod test {
    use crate::array::{InterleavedCoordBuffer, SeparatedCoordBuffer};
    use crate::trait_::GeometryArrayAccessor;

    /// Test Eq where the current index is true but another index is false
    #[test]
    fn test_eq_other_index_false() {
        let coords1 = vec![0., 3., 1., 4., 2., 5.];
        let buf1 = InterleavedCoordBuffer::new(coords1.into());
        let coord1 = buf1.value(0);

        let coords2 = vec![0., 3., 100., 400., 200., 500.];
        let buf2 = InterleavedCoordBuffer::new(coords2.into());
        let coord2 = buf2.value(0);

        assert_eq!(coord1, coord2);
    }

    #[test]
    fn test_eq_against_separated_coord() {
        let coords1 = vec![0., 3., 1., 4., 2., 5.];
        let buf1 = InterleavedCoordBuffer::new(coords1.into());
        let coord1 = buf1.value(0);

        let x = vec![0.];
        let y = vec![3.];
        let buf2 = SeparatedCoordBuffer::new(x.into(), y.into());
        let coord2 = buf2.value(0);

        assert_eq!(coord1, coord2);
    }
}
