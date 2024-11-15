use crate::algorithm::native::bounding_rect::bounding_rect_multipoint;
use crate::algorithm::native::eq::multi_point_eq;
use crate::array::util::OffsetBufferUtils;
use crate::array::{CoordBuffer, MultiPointArray};
use crate::datatypes::Dimension;
use crate::io::geo::multi_point_to_geo;
use crate::scalar::Point;
use crate::trait_::NativeScalar;
use arrow_buffer::OffsetBuffer;
use geo_traits::MultiPointTrait;
use rstar::{RTreeObject, AABB};

/// An Arrow equivalent of a MultiPoint
#[derive(Debug, Clone)]
pub struct MultiPoint<'a> {
    /// Buffer of coordinates
    pub(crate) coords: &'a CoordBuffer,

    /// Offsets into the coordinate array where each geometry starts
    pub(crate) geom_offsets: &'a OffsetBuffer<i32>,

    pub(crate) geom_index: usize,

    start_offset: usize,
}

impl<'a> MultiPoint<'a> {
    pub fn new(
        coords: &'a CoordBuffer,
        geom_offsets: &'a OffsetBuffer<i32>,
        geom_index: usize,
    ) -> Self {
        let (start_offset, _) = geom_offsets.start_end(geom_index);
        Self {
            coords,
            geom_offsets,
            geom_index,
            start_offset,
        }
    }

    pub fn into_owned_inner(self) -> (CoordBuffer, OffsetBuffer<i32>, usize) {
        let arr = MultiPointArray::new(
            self.coords.clone(),
            self.geom_offsets.clone(),
            None,
            Default::default(),
        );
        let sliced_arr = arr.owned_slice(self.geom_index, 1);
        let (coords, geom_offsets, _validity) = sliced_arr.into_inner();
        (coords, geom_offsets, 0)
    }
}

impl<'a> NativeScalar for MultiPoint<'a> {
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

impl<'a> MultiPointTrait for MultiPoint<'a> {
    type T = f64;
    type PointType<'b> = Point<'a> where Self: 'b;

    fn dim(&self) -> geo_traits::Dimensions {
        match self.coords.dim() {
            Dimension::XY => geo_traits::Dimensions::Xy,
            Dimension::XYZ => geo_traits::Dimensions::Xyz,
        }
    }

    fn num_points(&self) -> usize {
        let (start, end) = self.geom_offsets.start_end(self.geom_index);
        end - start
    }

    unsafe fn point_unchecked(&self, i: usize) -> Self::PointType<'_> {
        Point::new(self.coords, self.start_offset + i)
    }
}

impl<'a> MultiPointTrait for &'a MultiPoint<'a> {
    type T = f64;
    type PointType<'b> = Point<'a> where Self: 'b;

    fn dim(&self) -> geo_traits::Dimensions {
        match self.coords.dim() {
            Dimension::XY => geo_traits::Dimensions::Xy,
            Dimension::XYZ => geo_traits::Dimensions::Xyz,
        }
    }

    fn num_points(&self) -> usize {
        let (start, end) = self.geom_offsets.start_end(self.geom_index);
        end - start
    }

    unsafe fn point_unchecked(&self, i: usize) -> Self::PointType<'_> {
        Point::new(self.coords, self.start_offset + i)
    }
}

impl From<MultiPoint<'_>> for geo::MultiPoint {
    fn from(value: MultiPoint<'_>) -> Self {
        (&value).into()
    }
}

impl From<&MultiPoint<'_>> for geo::MultiPoint {
    fn from(value: &MultiPoint<'_>) -> Self {
        multi_point_to_geo(value)
    }
}

impl From<MultiPoint<'_>> for geo::Geometry {
    fn from(value: MultiPoint<'_>) -> Self {
        geo::Geometry::MultiPoint(value.into())
    }
}

impl RTreeObject for MultiPoint<'_> {
    type Envelope = AABB<[f64; 2]>;

    fn envelope(&self) -> Self::Envelope {
        let (lower, upper) = bounding_rect_multipoint(self);
        AABB::from_corners(lower, upper)
    }
}

impl<G: MultiPointTrait<T = f64>> PartialEq<G> for MultiPoint<'_> {
    fn eq(&self, other: &G) -> bool {
        multi_point_eq(self, other)
    }
}

#[cfg(test)]
mod test {
    use crate::array::MultiPointArray;
    use crate::test::multipoint::{mp0, mp1};
    use crate::trait_::ArrayAccessor;

    /// Test Eq where the current index is true but another index is false
    #[test]
    fn test_eq_other_index_false() {
        let arr1: MultiPointArray = vec![mp0(), mp1()].as_slice().into();
        let arr2: MultiPointArray = vec![mp0(), mp0()].as_slice().into();

        assert_eq!(arr1.value(0), arr2.value(0));
        assert_ne!(arr1.value(1), arr2.value(1));
    }
}
