use crate::algorithm::native::bounding_rect::bounding_rect_multipoint;
use crate::algorithm::native::eq::multi_point_eq;
use crate::array::util::OffsetBufferUtils;
use crate::array::{CoordBuffer, MultiPointArray};
use crate::geo_traits::MultiPointTrait;
use crate::io::geo::multi_point_to_geo;
use crate::scalar::Point;
use crate::trait_::GeometryArraySelfMethods;
use crate::trait_::GeometryScalarTrait;
use arrow_array::OffsetSizeTrait;
use arrow_buffer::OffsetBuffer;
use rstar::{RTreeObject, AABB};

/// An Arrow equivalent of a MultiPoint
#[derive(Debug, Clone)]
pub struct MultiPoint<'a, O: OffsetSizeTrait, const D: usize> {
    /// Buffer of coordinates
    pub(crate) coords: &'a CoordBuffer<D>,

    /// Offsets into the coordinate array where each geometry starts
    pub(crate) geom_offsets: &'a OffsetBuffer<O>,

    pub(crate) geom_index: usize,

    start_offset: usize,
}

impl<'a, O: OffsetSizeTrait, const D: usize> MultiPoint<'a, O, D> {
    pub fn new(
        coords: &'a CoordBuffer<D>,
        geom_offsets: &'a OffsetBuffer<O>,
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

    pub fn into_owned_inner(self) -> (CoordBuffer<D>, OffsetBuffer<O>, usize) {
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

impl<'a, O: OffsetSizeTrait, const D: usize> GeometryScalarTrait for MultiPoint<'a, O, D> {
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

impl<'a, O: OffsetSizeTrait, const D: usize> MultiPointTrait for MultiPoint<'a, O, D> {
    type T = f64;
    type ItemType<'b> = Point<'a, D> where Self: 'b;

    fn dim(&self) -> usize {
        D
    }

    fn num_points(&self) -> usize {
        let (start, end) = self.geom_offsets.start_end(self.geom_index);
        end - start
    }

    unsafe fn point_unchecked(&self, i: usize) -> Self::ItemType<'_> {
        Point::new(self.coords, self.start_offset + i)
    }
}

impl<'a, O: OffsetSizeTrait, const D: usize> MultiPointTrait for &'a MultiPoint<'a, O, D> {
    type T = f64;
    type ItemType<'b> = Point<'a, D> where Self: 'b;

    fn dim(&self) -> usize {
        D
    }

    fn num_points(&self) -> usize {
        let (start, end) = self.geom_offsets.start_end(self.geom_index);
        end - start
    }

    unsafe fn point_unchecked(&self, i: usize) -> Self::ItemType<'_> {
        Point::new(self.coords, self.start_offset + i)
    }
}

impl<O: OffsetSizeTrait, const D: usize> From<MultiPoint<'_, O, D>> for geo::MultiPoint {
    fn from(value: MultiPoint<'_, O, D>) -> Self {
        (&value).into()
    }
}

impl<O: OffsetSizeTrait, const D: usize> From<&MultiPoint<'_, O, D>> for geo::MultiPoint {
    fn from(value: &MultiPoint<'_, O, D>) -> Self {
        multi_point_to_geo(value)
    }
}

impl<O: OffsetSizeTrait, const D: usize> From<MultiPoint<'_, O, D>> for geo::Geometry {
    fn from(value: MultiPoint<'_, O, D>) -> Self {
        geo::Geometry::MultiPoint(value.into())
    }
}

impl<O: OffsetSizeTrait> RTreeObject for MultiPoint<'_, O, 2> {
    type Envelope = AABB<[f64; 2]>;

    fn envelope(&self) -> Self::Envelope {
        let (lower, upper) = bounding_rect_multipoint(self);
        AABB::from_corners(lower, upper)
    }
}

impl<O: OffsetSizeTrait, const D: usize, G: MultiPointTrait<T = f64>> PartialEq<G>
    for MultiPoint<'_, O, D>
{
    fn eq(&self, other: &G) -> bool {
        multi_point_eq(self, other)
    }
}

#[cfg(test)]
mod test {
    use crate::array::MultiPointArray;
    use crate::test::multipoint::{mp0, mp1};
    use crate::trait_::GeometryArrayAccessor;

    /// Test Eq where the current index is true but another index is false
    #[test]
    fn test_eq_other_index_false() {
        let arr1: MultiPointArray<i32, 2> = vec![mp0(), mp1()].as_slice().into();
        let arr2: MultiPointArray<i32, 2> = vec![mp0(), mp0()].as_slice().into();

        assert_eq!(arr1.value(0), arr2.value(0));
        assert_ne!(arr1.value(1), arr2.value(1));
    }
}
