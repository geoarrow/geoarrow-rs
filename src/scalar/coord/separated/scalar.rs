use crate::algorithm::native::eq::coord_eq;
use crate::io::geo::coord_to_geo;
use crate::scalar::InterleavedCoord;
use crate::trait_::NativeScalar;
use arrow_buffer::ScalarBuffer;
use geo_traits::CoordTrait;
use rstar::{RTreeObject, AABB};

#[derive(Debug, Clone)]
pub struct SeparatedCoord<'a, const D: usize> {
    pub(crate) buffers: &'a [ScalarBuffer<f64>; D],
    pub(crate) i: usize,
}

impl<'a, const D: usize> SeparatedCoord<'a, D> {
    /// Return `true` if all values in the coordinate are f64::NAN
    pub(crate) fn is_nan(&self) -> bool {
        (0..D).all(|coord_dim| self.nth_unchecked(coord_dim).is_nan())
    }
}

impl<'a, const D: usize> NativeScalar for SeparatedCoord<'a, D> {
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

impl<const D: usize> From<SeparatedCoord<'_, D>> for geo::Coord {
    fn from(value: SeparatedCoord<D>) -> Self {
        (&value).into()
    }
}
impl<const D: usize> From<&SeparatedCoord<'_, D>> for geo::Coord {
    fn from(value: &SeparatedCoord<D>) -> Self {
        coord_to_geo(value)
    }
}

impl<const D: usize> From<SeparatedCoord<'_, D>> for geo::Point {
    fn from(value: SeparatedCoord<'_, D>) -> Self {
        (&value).into()
    }
}

impl<const D: usize> From<&SeparatedCoord<'_, D>> for geo::Point {
    fn from(value: &SeparatedCoord<'_, D>) -> Self {
        let coord: geo::Coord = value.into();
        coord.into()
    }
}

impl<const D: usize> RTreeObject for SeparatedCoord<'_, D> {
    type Envelope = AABB<[f64; 2]>;

    fn envelope(&self) -> Self::Envelope {
        AABB::from_point([self.x(), self.y()])
    }
}

impl<const D: usize> PartialEq for SeparatedCoord<'_, D> {
    fn eq(&self, other: &SeparatedCoord<D>) -> bool {
        coord_eq(self, other)
    }
}

impl<const D: usize> PartialEq<InterleavedCoord<'_, D>> for SeparatedCoord<'_, D> {
    fn eq(&self, other: &InterleavedCoord<D>) -> bool {
        coord_eq(self, other)
    }
}

impl<const D: usize> CoordTrait for SeparatedCoord<'_, D> {
    type T = f64;

    fn dim(&self) -> geo_traits::Dimensions {
        // TODO: pass through field information from array
        match D {
            2 => geo_traits::Dimensions::Xy,
            3 => geo_traits::Dimensions::Xyz,
            _ => todo!(),
        }
    }

    fn nth_unchecked(&self, n: usize) -> Self::T {
        self.buffers[n][self.i]
    }

    fn x(&self) -> Self::T {
        self.buffers[0][self.i]
    }

    fn y(&self) -> Self::T {
        self.buffers[1][self.i]
    }
}

impl<const D: usize> CoordTrait for &SeparatedCoord<'_, D> {
    type T = f64;

    fn dim(&self) -> geo_traits::Dimensions {
        // TODO: pass through field information from array
        match D {
            2 => geo_traits::Dimensions::Xy,
            3 => geo_traits::Dimensions::Xyz,
            _ => todo!(),
        }
    }

    fn nth_unchecked(&self, n: usize) -> Self::T {
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
    use crate::array::{InterleavedCoordBuffer, SeparatedCoordBuffer};

    /// Test Eq where the current index is true but another index is false
    #[test]
    fn test_eq_other_index_false() {
        let x1 = vec![0., 1., 2.];
        let y1 = vec![3., 4., 5.];
        let buf1 = SeparatedCoordBuffer::new([x1.into(), y1.into()]);
        let coord1 = buf1.value(0);

        let x2 = vec![0., 100., 2.];
        let y2 = vec![3., 400., 5.];
        let buf2 = SeparatedCoordBuffer::new([x2.into(), y2.into()]);
        let coord2 = buf2.value(0);

        assert_eq!(coord1, coord2);
    }

    #[test]
    fn test_eq_against_interleaved_coord() {
        let x1 = vec![0., 1., 2.];
        let y1 = vec![3., 4., 5.];
        let buf1 = SeparatedCoordBuffer::new([x1.into(), y1.into()]);
        let coord1 = buf1.value(0);

        let coords2 = vec![0., 3., 1., 4., 2., 5.];
        let buf2 = InterleavedCoordBuffer::new(coords2.into());
        let coord2 = buf2.value(0);

        assert_eq!(coord1, coord2);
    }
}
