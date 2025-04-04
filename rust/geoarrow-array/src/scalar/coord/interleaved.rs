use arrow_buffer::ScalarBuffer;
use rstar::{RTreeObject, AABB};

use crate::algorithm::native::eq::coord_eq;
use crate::scalar::SeparatedCoord;
use geo_traits::CoordTrait;
use geoarrow_schema::Dimension;

/// An Arrow equivalent of a Coord
///
/// This implements [CoordTrait], which you can use to extract data.
#[derive(Debug, Clone)]
pub struct InterleavedCoord<'a> {
    pub(crate) coords: &'a ScalarBuffer<f64>,
    pub(crate) i: usize,
    pub(crate) dim: Dimension,
}

impl InterleavedCoord<'_> {
    /// Return `true` if all values in the coordinate are f64::NAN
    pub(crate) fn is_nan(&self) -> bool {
        (0..self.dim.size()).all(|coord_dim| self.nth_or_panic(coord_dim).is_nan())
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

    fn dim(&self) -> geo_traits::Dimensions {
        self.dim.into()
    }

    fn nth_or_panic(&self, n: usize) -> Self::T {
        debug_assert!(n < self.dim.size());
        *self.coords.get(self.i * self.dim.size() + n).unwrap()
    }

    fn x(&self) -> Self::T {
        *self.coords.get(self.i * self.dim.size()).unwrap()
    }

    fn y(&self) -> Self::T {
        *self.coords.get(self.i * self.dim.size() + 1).unwrap()
    }
}

impl CoordTrait for &InterleavedCoord<'_> {
    type T = f64;

    fn dim(&self) -> geo_traits::Dimensions {
        self.dim.into()
    }

    fn nth_or_panic(&self, n: usize) -> Self::T {
        debug_assert!(n < self.dim.size());
        *self.coords.get(self.i * self.dim.size() + n).unwrap()
    }

    fn x(&self) -> Self::T {
        *self.coords.get(self.i * self.dim.size()).unwrap()
    }

    fn y(&self) -> Self::T {
        *self.coords.get(self.i * self.dim.size() + 1).unwrap()
    }
}

#[cfg(test)]
mod test {
    use arrow_buffer::ScalarBuffer;

    use crate::array::{InterleavedCoordBuffer, SeparatedCoordBuffer};
    use geoarrow_schema::Dimension;

    /// Test Eq where the current index is true but another index is false
    #[test]
    fn test_eq_other_index_false() {
        let coords1 = vec![0., 3., 1., 4., 2., 5.];
        let buf1 = InterleavedCoordBuffer::new(coords1.into(), Dimension::XY);
        let coord1 = buf1.value(0);

        let coords2 = vec![0., 3., 100., 400., 200., 500.];
        let buf2 = InterleavedCoordBuffer::new(coords2.into(), Dimension::XY);
        let coord2 = buf2.value(0);

        assert_eq!(coord1, coord2);
    }

    #[test]
    fn test_eq_against_separated_coord() {
        let coords1 = vec![0., 3., 1., 4., 2., 5.];
        let buf1 = InterleavedCoordBuffer::new(coords1.into(), Dimension::XY);
        let coord1 = buf1.value(0);

        let x = vec![0.];
        let y = vec![3.];
        let buf2 = SeparatedCoordBuffer::new(
            [
                x.into(),
                y.into(),
                ScalarBuffer::from(vec![]),
                ScalarBuffer::from(vec![]),
            ],
            Dimension::XY,
        );
        let coord2 = buf2.value(0);

        assert_eq!(coord1, coord2);
    }
}
