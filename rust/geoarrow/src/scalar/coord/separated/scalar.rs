use crate::algorithm::native::eq::coord_eq;
use crate::datatypes::Dimension;
use crate::scalar::InterleavedCoord;
use crate::trait_::NativeScalar;
use arrow_buffer::ScalarBuffer;
use geo_traits::to_geo::ToGeoCoord;
use geo_traits::CoordTrait;
use rstar::{RTreeObject, AABB};

#[derive(Debug, Clone)]
pub struct SeparatedCoord<'a> {
    pub(crate) buffers: &'a [ScalarBuffer<f64>; 4],
    pub(crate) i: usize,
    pub(crate) dim: Dimension,
}

impl<'a> SeparatedCoord<'a> {
    /// Return `true` if all values in the coordinate are f64::NAN
    pub(crate) fn is_nan(&self) -> bool {
        (0..self.dim.size()).all(|coord_dim| self.nth_or_panic(coord_dim).is_nan())
    }
}

impl<'a> NativeScalar for SeparatedCoord<'a> {
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

impl From<SeparatedCoord<'_>> for geo::Coord {
    fn from(value: SeparatedCoord) -> Self {
        (&value).into()
    }
}
impl From<&SeparatedCoord<'_>> for geo::Coord {
    fn from(value: &SeparatedCoord) -> Self {
        value.to_coord()
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

    fn dim(&self) -> geo_traits::Dimensions {
        self.dim.into()
    }

    fn nth_or_panic(&self, n: usize) -> Self::T {
        self.buffers[n][self.i]
    }

    fn x(&self) -> Self::T {
        self.buffers[0][self.i]
    }

    fn y(&self) -> Self::T {
        self.buffers[1][self.i]
    }
}

impl CoordTrait for &SeparatedCoord<'_> {
    type T = f64;

    fn dim(&self) -> geo_traits::Dimensions {
        self.dim.into()
    }

    fn nth_or_panic(&self, n: usize) -> Self::T {
        self.buffers[n][self.i]
    }

    fn x(&self) -> Self::T {
        self.buffers[0][self.i]
    }

    fn y(&self) -> Self::T {
        self.buffers[1][self.i]
    }
}

#[cfg(test)]
mod test {
    use arrow_buffer::ScalarBuffer;

    use crate::array::{InterleavedCoordBuffer, SeparatedCoordBuffer};
    use crate::datatypes::Dimension;

    /// Test Eq where the current index is true but another index is false
    #[test]
    fn test_eq_other_index_false() {
        let x1 = vec![0., 1., 2.];
        let y1 = vec![3., 4., 5.];
        let buf1 = SeparatedCoordBuffer::new(
            [
                x1.into(),
                y1.into(),
                ScalarBuffer::from(vec![]),
                ScalarBuffer::from(vec![]),
            ],
            Dimension::XY,
        );
        let coord1 = buf1.value(0);

        let x2 = vec![0., 100., 2.];
        let y2 = vec![3., 400., 5.];
        let buf2 = SeparatedCoordBuffer::new(
            [
                x2.into(),
                y2.into(),
                ScalarBuffer::from(vec![]),
                ScalarBuffer::from(vec![]),
            ],
            Dimension::XY,
        );
        let coord2 = buf2.value(0);

        assert_eq!(coord1, coord2);
    }

    #[test]
    fn test_eq_against_interleaved_coord() {
        let x1 = vec![0., 1., 2.];
        let y1 = vec![3., 4., 5.];
        let buf1 = SeparatedCoordBuffer::new(
            [
                x1.into(),
                y1.into(),
                ScalarBuffer::from(vec![]),
                ScalarBuffer::from(vec![]),
            ],
            Dimension::XY,
        );
        let coord1 = buf1.value(0);

        let coords2 = vec![0., 3., 1., 4., 2., 5.];
        let buf2 = InterleavedCoordBuffer::new(coords2.into(), Dimension::XY);
        let coord2 = buf2.value(0);

        assert_eq!(coord1, coord2);
    }
}
