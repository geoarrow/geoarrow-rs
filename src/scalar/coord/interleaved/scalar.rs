use arrow_buffer::ScalarBuffer;
use rstar::{RTreeObject, AABB};

use crate::algorithm::native::eq::coord_eq;
use crate::geo_traits::CoordTrait;
use crate::io::geo::coord_to_geo;
use crate::scalar::SeparatedCoord;
use crate::trait_::NativeScalar;

#[derive(Debug, Clone)]
pub struct InterleavedCoord<'a, const D: usize> {
    pub(crate) coords: &'a ScalarBuffer<f64>,
    pub(crate) i: usize,
}

impl<'a, const D: usize> NativeScalar for InterleavedCoord<'a, D> {
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

impl<const D: usize> From<InterleavedCoord<'_, D>> for geo::Coord {
    fn from(value: InterleavedCoord<D>) -> Self {
        (&value).into()
    }
}

impl<const D: usize> From<&InterleavedCoord<'_, D>> for geo::Coord {
    fn from(value: &InterleavedCoord<D>) -> Self {
        coord_to_geo(value)
    }
}

impl<const D: usize> From<InterleavedCoord<'_, D>> for geo::Point {
    fn from(value: InterleavedCoord<'_, D>) -> Self {
        (&value).into()
    }
}

impl<const D: usize> From<&InterleavedCoord<'_, D>> for geo::Point {
    fn from(value: &InterleavedCoord<'_, D>) -> Self {
        let coord: geo::Coord = value.into();
        coord.into()
    }
}

impl<const D: usize> RTreeObject for InterleavedCoord<'_, D> {
    type Envelope = AABB<[f64; 2]>;

    fn envelope(&self) -> Self::Envelope {
        AABB::from_point([self.x(), self.y()])
    }
}

impl<const D: usize> PartialEq for InterleavedCoord<'_, D> {
    fn eq(&self, other: &Self) -> bool {
        coord_eq(self, other)
    }
}

impl<const D: usize> PartialEq<SeparatedCoord<'_, D>> for InterleavedCoord<'_, D> {
    fn eq(&self, other: &SeparatedCoord<'_, D>) -> bool {
        coord_eq(self, other)
    }
}

impl<const D: usize> CoordTrait for InterleavedCoord<'_, D> {
    type T = f64;

    fn dim(&self) -> usize {
        D
    }

    fn nth_unchecked(&self, n: usize) -> Self::T {
        debug_assert!(n < D);
        *self.coords.get(self.i * D + n).unwrap()
    }

    fn x(&self) -> Self::T {
        *self.coords.get(self.i * D).unwrap()
    }

    fn y(&self) -> Self::T {
        *self.coords.get(self.i * D + 1).unwrap()
    }
}

impl<const D: usize> CoordTrait for &InterleavedCoord<'_, D> {
    type T = f64;

    fn dim(&self) -> usize {
        D
    }

    fn nth_unchecked(&self, n: usize) -> Self::T {
        debug_assert!(n < D);
        *self.coords.get(self.i * D + n).unwrap()
    }

    fn x(&self) -> Self::T {
        *self.coords.get(self.i * D).unwrap()
    }

    fn y(&self) -> Self::T {
        *self.coords.get(self.i * D + 1).unwrap()
    }
}

#[cfg(test)]
mod test {
    use crate::array::{InterleavedCoordBuffer, SeparatedCoordBuffer};
    use crate::trait_::NativeArrayAccessor;

    /// Test Eq where the current index is true but another index is false
    #[test]
    fn test_eq_other_index_false() {
        let coords1 = vec![0., 3., 1., 4., 2., 5.];
        let buf1 = InterleavedCoordBuffer::<2>::new(coords1.into());
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
        let buf2 = SeparatedCoordBuffer::new([x.into(), y.into()]);
        let coord2 = buf2.value(0);

        assert_eq!(coord1, coord2);
    }
}
