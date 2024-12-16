use crate::algorithm::native::bounding_rect::bounding_rect_linestring;
use crate::algorithm::native::eq::line_string_eq;
use crate::array::util::OffsetBufferUtils;
use crate::array::LineStringArray;
use crate::scalar::Coord;
use crate::trait_::NativeScalar;
use crate::{ArrayBase, NativeArray};
use geo_traits::to_geo::ToGeoLineString;
use geo_traits::LineStringTrait;
use rstar::{RTreeObject, AABB};

/// An Arrow equivalent of a LineString
///
/// This is stored as a [LineStringArray] with length 1. That element may not be null.
#[derive(Debug, Clone)]
pub struct LineString {
    array: LineStringArray,
    start_offset: usize,
}

impl LineString {
    pub fn new(array: LineStringArray) -> Self {
        assert_eq!(array.len(), 1);
        assert!(!array.is_null(0));
        let (start_offset, _) = array.geom_offsets.start_end(0);
        Self {
            array,
            start_offset,
        }
    }

    pub fn into_inner(self) -> LineStringArray {
        self.array
    }
}

impl NativeScalar for LineString {
    type ScalarGeo = geo::LineString;

    fn to_geo(&self) -> Self::ScalarGeo {
        self.into()
    }

    fn to_geo_geometry(&self) -> geo::Geometry {
        geo::Geometry::LineString(self.to_geo())
    }

    #[cfg(feature = "geos")]
    fn to_geos(&self) -> std::result::Result<geos::Geometry, geos::Error> {
        self.try_into()
    }
}

impl<'a> LineStringTrait for LineString {
    type T = f64;
    type CoordType<'b>
        = Coord
    where
        Self: 'b;

    fn dim(&self) -> geo_traits::Dimensions {
        self.0.dimension().into()
    }

    fn num_coords(&self) -> usize {
        let (start, end) = self.0.geom_offsets.start_end(0);
        end - start
    }

    unsafe fn coord_unchecked(&self, i: usize) -> Self::CoordType<'_> {
        self.array.coords.value(self.start_offset + i)
    }
}

impl<'a> LineStringTrait for &'a LineString {
    type T = f64;
    type CoordType<'b>
        = Coord
    where
        Self: 'b;

    fn dim(&self) -> geo_traits::Dimensions {
        self.0.dimension().into()
    }

    fn num_coords(&self) -> usize {
        let (start, end) = self.0.geom_offsets.start_end(0);
        end - start
    }

    unsafe fn coord_unchecked(&self, i: usize) -> Self::CoordType<'_> {
        self.array.coords.value(self.start_offset + i)
    }
}

impl From<LineString> for geo::LineString {
    fn from(value: LineString) -> Self {
        (&value).into()
    }
}

impl From<&LineString> for geo::LineString {
    fn from(value: &LineString) -> Self {
        value.to_line_string()
    }
}

impl From<LineString> for geo::Geometry {
    fn from(value: LineString) -> Self {
        geo::Geometry::LineString(value.into())
    }
}

impl RTreeObject for LineString {
    type Envelope = AABB<[f64; 2]>;

    fn envelope(&self) -> Self::Envelope {
        let (lower, upper) = bounding_rect_linestring(self);
        AABB::from_corners(lower, upper)
    }
}

impl<G: LineStringTrait<T = f64>> PartialEq<G> for LineString {
    fn eq(&self, other: &G) -> bool {
        line_string_eq(self, other)
    }
}

#[cfg(test)]
mod test {
    use crate::array::LineStringArray;
    use crate::datatypes::Dimension;
    use crate::test::linestring::{ls0, ls1};
    use crate::trait_::ArrayAccessor;

    /// Test Eq where the current index is true but another index is false
    #[test]
    fn test_eq_other_index_false() {
        let arr1: LineStringArray = (vec![ls0(), ls1()].as_slice(), Dimension::XY).into();
        let arr2: LineStringArray = (vec![ls0(), ls0()].as_slice(), Dimension::XY).into();

        assert_eq!(arr1.value(0), arr2.value(0));
        assert_ne!(arr1.value(1), arr2.value(1));
    }
}
