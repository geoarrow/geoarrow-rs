use crate::algorithm::native::bounding_rect::bounding_rect_multipolygon;
use crate::algorithm::native::eq::multi_polygon_eq;
use crate::array::util::OffsetBufferUtils;
use crate::array::{CoordBuffer, MultiPolygonArray};
use crate::geo_traits::MultiPolygonTrait;
use crate::io::geo::multi_polygon_to_geo;
use crate::scalar::Polygon;
use crate::trait_::NativeScalar;
use arrow_array::OffsetSizeTrait;
use arrow_buffer::OffsetBuffer;
use rstar::{RTreeObject, AABB};

/// An Arrow equivalent of a MultiPolygon
#[derive(Debug, Clone)]
pub struct MultiPolygon<'a, const D: usize> {
    pub(crate) coords: &'a CoordBuffer<D>,

    /// Offsets into the polygon array where each geometry starts
    pub(crate) geom_offsets: &'a OffsetBuffer<i32>,

    /// Offsets into the ring array where each polygon starts
    pub(crate) polygon_offsets: &'a OffsetBuffer<i32>,

    /// Offsets into the coordinate array where each ring starts
    pub(crate) ring_offsets: &'a OffsetBuffer<i32>,

    pub(crate) geom_index: usize,

    start_offset: usize,
}

impl<'a, const D: usize> MultiPolygon<'a, D> {
    pub fn new(coords: &'a CoordBuffer<D>, geom_offsets: &'a OffsetBuffer<i32>, polygon_offsets: &'a OffsetBuffer<i32>, ring_offsets: &'a OffsetBuffer<i32>, geom_index: usize) -> Self {
        let (start_offset, _) = geom_offsets.start_end(geom_index);
        Self { coords, geom_offsets, polygon_offsets, ring_offsets, geom_index, start_offset }
    }

    pub fn into_owned_inner(self) -> (CoordBuffer<D>, OffsetBuffer<i32>, OffsetBuffer<i32>, OffsetBuffer<i32>, usize) {
        let arr = MultiPolygonArray::new(self.coords.clone(), self.geom_offsets.clone(), self.polygon_offsets.clone(), self.ring_offsets.clone(), None, Default::default());
        let sliced_arr = arr.owned_slice(self.geom_index, 1);
        let (coords, geom_offsets, polygon_offsets, ring_offsets) = sliced_arr.into_inner();

        (coords, geom_offsets, polygon_offsets, ring_offsets, 0)
    }
}

impl<'a, const D: usize> NativeScalar for MultiPolygon<'a, D> {
    type ScalarGeo = geo::MultiPolygon;

    fn to_geo(&self) -> Self::ScalarGeo {
        self.into()
    }

    fn to_geo_geometry(&self) -> geo::Geometry {
        geo::Geometry::MultiPolygon(self.to_geo())
    }

    #[cfg(feature = "geos")]
    fn to_geos(&self) -> std::result::Result<geos::Geometry, geos::Error> {
        self.try_into()
    }
}

impl<'a, const D: usize> MultiPolygonTrait for MultiPolygon<'a, D> {
    type T = f64;
    type ItemType<'b> = Polygon<'a, D> where Self: 'b;

    fn dim(&self) -> usize {
        D
    }

    fn num_polygons(&self) -> usize {
        let (start, end) = self.geom_offsets.start_end(self.geom_index);
        end - start
    }

    unsafe fn polygon_unchecked(&self, i: usize) -> Self::ItemType<'_> {
        Polygon::new(self.coords, self.polygon_offsets, self.ring_offsets, self.start_offset + i)
    }
}

impl<'a, const D: usize> MultiPolygonTrait for &'a MultiPolygon<'a, D> {
    type T = f64;
    type ItemType<'b> = Polygon<'a, D> where Self: 'b;

    fn dim(&self) -> usize {
        D
    }

    fn num_polygons(&self) -> usize {
        let (start, end) = self.geom_offsets.start_end(self.geom_index);
        end - start
    }

    unsafe fn polygon_unchecked(&self, i: usize) -> Self::ItemType<'_> {
        Polygon::new(self.coords, self.polygon_offsets, self.ring_offsets, self.start_offset + i)
    }
}

impl<const D: usize> From<MultiPolygon<'_, D>> for geo::MultiPolygon {
    fn from(value: MultiPolygon<'_, D>) -> Self {
        (&value).into()
    }
}

impl<const D: usize> From<&MultiPolygon<'_, D>> for geo::MultiPolygon {
    fn from(value: &MultiPolygon<'_, D>) -> Self {
        multi_polygon_to_geo(value)
    }
}

impl<const D: usize> From<MultiPolygon<'_, D>> for geo::Geometry {
    fn from(value: MultiPolygon<'_, D>) -> Self {
        geo::Geometry::MultiPolygon(value.into())
    }
}

impl RTreeObject for MultiPolygon<'_, 2> {
    type Envelope = AABB<[f64; 2]>;

    fn envelope(&self) -> Self::Envelope {
        let (lower, upper) = bounding_rect_multipolygon(self);
        AABB::from_corners(lower, upper)
    }
}

impl<const D: usize, G: MultiPolygonTrait<T = f64>> PartialEq<G> for MultiPolygon<'_, D> {
    fn eq(&self, other: &G) -> bool {
        multi_polygon_eq(self, other)
    }
}

#[cfg(test)]
mod test {
    use crate::array::MultiPolygonArray;
    use crate::test::multipolygon::{mp0, mp1};
    use crate::trait_::ArrayAccessor;

    /// Test Eq where the current index is true but another index is false
    #[test]
    fn test_eq_other_index_false() {
        let arr1: MultiPolygonArray<i32, 2> = vec![mp0(), mp1()].as_slice().into();
        let arr2: MultiPolygonArray<i32, 2> = vec![mp0(), mp0()].as_slice().into();

        assert_eq!(arr1.value(0), arr2.value(0));
        assert_ne!(arr1.value(1), arr2.value(1));
    }
}
