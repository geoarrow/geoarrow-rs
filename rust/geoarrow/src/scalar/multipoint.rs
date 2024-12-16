use crate::algorithm::native::bounding_rect::bounding_rect_multipoint;
use crate::algorithm::native::eq::multi_point_eq;
use crate::array::util::OffsetBufferUtils;
use crate::array::{MultiPointArray, PointArray};
use crate::scalar::Point;
use crate::trait_::{ArrayAccessor, NativeScalar};
use crate::ArrayBase;
use geo_traits::to_geo::ToGeoMultiPoint;
use geo_traits::MultiPointTrait;
use rstar::{RTreeObject, AABB};

/// An Arrow equivalent of a MultiPoint
#[derive(Debug, Clone)]
pub struct MultiPoint {
    array: MultiPointArray,
    start_offset: usize,
}

impl MultiPoint {
    pub fn new(array: MultiPointArray) -> Self {
        assert_eq!(array.len(), 1);
        assert!(!array.is_null(0));
        let (start_offset, _) = array.geom_offsets.start_end(0);
        Self {
            array,
            start_offset,
        }
    }

    pub fn into_inner(self) -> MultiPointArray {
        self.array
    }
}

impl NativeScalar for MultiPoint {
    type ScalarGeo = geo::MultiPoint;

    fn to_geo(&self) -> Self::ScalarGeo {
        self.into()
    }

    fn to_geo_geometry(&self) -> geo::Geometry {
        geo::Geometry::MultiPoint(self.to_geo())
    }

    #[cfg(feature = "geos")]
    fn to_geos(&self) -> std::result::Result<geos::Geometry, geos::Error> {
        self.try_into()
    }
}

impl MultiPointTrait for MultiPoint {
    type T = f64;
    type PointType<'b>
        = Point
    where
        Self: 'b;

    fn dim(&self) -> geo_traits::Dimensions {
        self.array.coords.dim().into()
    }

    fn num_points(&self) -> usize {
        let (start, end) = self.array.geom_offsets.start_end(0);
        end - start
    }

    unsafe fn point_unchecked(&self, i: usize) -> Self::PointType<'_> {
        let arr = PointArray::new(self.array.coords.clone(), None, Default::default());
        arr.value(self.start_offset + i)
    }
}

impl MultiPointTrait for &MultiPoint {
    type T = f64;
    type PointType<'b>
        = Point
    where
        Self: 'b;

    fn dim(&self) -> geo_traits::Dimensions {
        self.array.coords.dim().into()
    }

    fn num_points(&self) -> usize {
        let (start, end) = self.array.geom_offsets.start_end(0);
        end - start
    }

    unsafe fn point_unchecked(&self, i: usize) -> Self::PointType<'_> {
        let arr = PointArray::new(self.array.coords.clone(), None, Default::default());
        arr.value(self.start_offset + i)
    }
}

impl From<MultiPoint> for geo::MultiPoint {
    fn from(value: MultiPoint) -> Self {
        (&value).into()
    }
}

impl From<&MultiPoint> for geo::MultiPoint {
    fn from(value: &MultiPoint) -> Self {
        value.to_multi_point()
    }
}

impl From<MultiPoint> for geo::Geometry {
    fn from(value: MultiPoint) -> Self {
        geo::Geometry::MultiPoint(value.into())
    }
}

impl RTreeObject for MultiPoint {
    type Envelope = AABB<[f64; 2]>;

    fn envelope(&self) -> Self::Envelope {
        let (lower, upper) = bounding_rect_multipoint(self);
        AABB::from_corners(lower, upper)
    }
}

impl<G: MultiPointTrait<T = f64>> PartialEq<G> for MultiPoint {
    fn eq(&self, other: &G) -> bool {
        multi_point_eq(self, other)
    }
}

#[cfg(test)]
mod test {
    use crate::array::MultiPointArray;
    use crate::datatypes::Dimension;
    use crate::test::multipoint::{mp0, mp1};
    use crate::trait_::ArrayAccessor;

    /// Test Eq where the current index is true but another index is false
    #[test]
    fn test_eq_other_index_false() {
        let arr1: MultiPointArray = (vec![mp0(), mp1()].as_slice(), Dimension::XY).into();
        let arr2: MultiPointArray = (vec![mp0(), mp0()].as_slice(), Dimension::XY).into();

        assert_eq!(arr1.value(0), arr2.value(0));
        assert_ne!(arr1.value(1), arr2.value(1));
    }
}
