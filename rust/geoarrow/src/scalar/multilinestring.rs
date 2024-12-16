use crate::algorithm::native::bounding_rect::bounding_rect_multilinestring;
use crate::algorithm::native::eq::multi_line_string_eq;
use crate::array::util::OffsetBufferUtils;
use crate::array::{LineStringArray, MultiLineStringArray};
use crate::scalar::LineString;
use crate::trait_::{ArrayAccessor, NativeScalar};
use crate::ArrayBase;
use geo_traits::to_geo::ToGeoMultiLineString;
use geo_traits::MultiLineStringTrait;
use rstar::{RTreeObject, AABB};

/// An Arrow equivalent of a MultiLineString
#[derive(Debug, Clone)]
pub struct MultiLineString {
    array: MultiLineStringArray,
    start_offset: usize,
}

impl MultiLineString {
    pub fn new(array: MultiLineStringArray) -> Self {
        assert_eq!(array.len(), 1);
        assert!(!array.is_null(0));
        let (start_offset, _) = array.geom_offsets.start_end(0);
        Self {
            array,
            start_offset,
        }
    }

    pub fn into_inner(self) -> MultiLineStringArray {
        self.array
    }
}

impl NativeScalar for MultiLineString {
    type ScalarGeo = geo::MultiLineString;

    fn to_geo(&self) -> Self::ScalarGeo {
        self.into()
    }

    fn to_geo_geometry(&self) -> geo::Geometry {
        geo::Geometry::MultiLineString(self.to_geo())
    }

    #[cfg(feature = "geos")]
    fn to_geos(&self) -> std::result::Result<geos::Geometry, geos::Error> {
        self.try_into()
    }
}

impl<'a> MultiLineStringTrait for MultiLineString {
    type T = f64;
    type LineStringType<'b>
        = LineString
    where
        Self: 'b;

    fn dim(&self) -> geo_traits::Dimensions {
        self.array.coords.dim().into()
    }

    fn num_line_strings(&self) -> usize {
        let (start, end) = self.array.geom_offsets.start_end(0);
        end - start
    }

    unsafe fn line_string_unchecked(&self, i: usize) -> Self::LineStringType<'_> {
        let arr = LineStringArray::new(
            self.array.coords.clone(),
            self.array.ring_offsets.clone(),
            None,
            Default::default(),
        );
        arr.value(self.start_offset + i)
    }
}

impl<'a> MultiLineStringTrait for &'a MultiLineString {
    type T = f64;
    type LineStringType<'b>
        = LineString
    where
        Self: 'b;

    fn dim(&self) -> geo_traits::Dimensions {
        self.array.coords.dim().into()
    }

    fn num_line_strings(&self) -> usize {
        let (start, end) = self.array.geom_offsets.start_end(0);
        end - start
    }

    unsafe fn line_string_unchecked(&self, i: usize) -> Self::LineStringType<'_> {
        let arr = LineStringArray::new(
            self.array.coords.clone(),
            self.array.ring_offsets.clone(),
            None,
            Default::default(),
        );
        arr.value(self.start_offset + i)
    }
}

impl From<MultiLineString> for geo::MultiLineString {
    fn from(value: MultiLineString) -> Self {
        (&value).into()
    }
}

impl From<&MultiLineString> for geo::MultiLineString {
    fn from(value: &MultiLineString) -> Self {
        value.to_multi_line_string()
    }
}

impl From<MultiLineString> for geo::Geometry {
    fn from(value: MultiLineString) -> Self {
        geo::Geometry::MultiLineString(value.into())
    }
}

impl RTreeObject for MultiLineString {
    type Envelope = AABB<[f64; 2]>;

    fn envelope(&self) -> Self::Envelope {
        let (lower, upper) = bounding_rect_multilinestring(self);
        AABB::from_corners(lower, upper)
    }
}

impl<G: MultiLineStringTrait<T = f64>> PartialEq<G> for MultiLineString {
    fn eq(&self, other: &G) -> bool {
        multi_line_string_eq(self, other)
    }
}

#[cfg(test)]
mod test {
    use crate::array::MultiLineStringArray;
    use crate::datatypes::Dimension;
    use crate::test::multilinestring::{ml0, ml1};
    use crate::trait_::ArrayAccessor;

    /// Test Eq where the current index is true but another index is false
    #[test]
    fn test_eq_other_index_false() {
        let arr1: MultiLineStringArray = (vec![ml0(), ml1()].as_slice(), Dimension::XY).into();
        let arr2: MultiLineStringArray = (vec![ml0(), ml0()].as_slice(), Dimension::XY).into();

        assert_eq!(arr1.value(0), arr2.value(0));
        assert_ne!(arr1.value(1), arr2.value(1));
    }
}
