use crate::algorithm::native::eq::multi_polygon_eq;
use crate::array::{CoordBuffer, MultiPolygonArray};
use crate::geo_traits::MultiPolygonTrait;
use crate::scalar::{MultiPolygon, Polygon};
use arrow_buffer::OffsetBuffer;

#[derive(Clone, Debug)]
pub struct OwnedMultiPolygon<const D: usize> {
    coords: CoordBuffer<D>,

    /// Offsets into the coordinate array where each geometry starts
    geom_offsets: OffsetBuffer<i32>,

    polygon_offsets: OffsetBuffer<i32>,

    ring_offsets: OffsetBuffer<i32>,

    geom_index: usize,
}

impl<const D: usize> OwnedMultiPolygon<D> {
    pub fn new(coords: CoordBuffer<D>, geom_offsets: OffsetBuffer<i32>, polygon_offsets: OffsetBuffer<i32>, ring_offsets: OffsetBuffer<i32>, geom_index: usize) -> Self {
        Self { coords, geom_offsets, polygon_offsets, ring_offsets, geom_index }
    }
}

impl<'a, const D: usize> From<&'a OwnedMultiPolygon<D>> for MultiPolygon<'a, D> {
    fn from(value: &'a OwnedMultiPolygon<D>) -> Self {
        Self::new(&value.coords, &value.geom_offsets, &value.polygon_offsets, &value.ring_offsets, value.geom_index)
    }
}

impl From<OwnedMultiPolygon<2>> for geo::MultiPolygon {
    fn from(value: OwnedMultiPolygon<2>) -> Self {
        let geom = MultiPolygon::from(&value);
        geom.into()
    }
}

impl<'a, const D: usize> From<MultiPolygon<'a, D>> for OwnedMultiPolygon<D> {
    fn from(value: MultiPolygon<'a, D>) -> Self {
        let (coords, geom_offsets, polygon_offsets, ring_offsets, geom_index) = value.into_owned_inner();
        Self::new(coords, geom_offsets, polygon_offsets, ring_offsets, geom_index)
    }
}

impl<const D: usize> From<OwnedMultiPolygon<D>> for MultiPolygonArray<D> {
    fn from(value: OwnedMultiPolygon<D>) -> Self {
        Self::new(value.coords, value.geom_offsets, value.polygon_offsets, value.ring_offsets, None, Default::default())
    }
}

impl<const D: usize> MultiPolygonTrait for OwnedMultiPolygon<D> {
    type T = f64;
    type ItemType<'b> = Polygon<'b, D> where Self: 'b;

    fn dim(&self) -> usize {
        D
    }

    fn num_polygons(&self) -> usize {
        MultiPolygon::from(self).num_polygons()
    }

    unsafe fn polygon_unchecked(&self, i: usize) -> Self::ItemType<'_> {
        MultiPolygon::from(self).polygon_unchecked(i)
    }
}

impl<G: MultiPolygonTrait<T = f64>> PartialEq<G> for OwnedMultiPolygon<2> {
    fn eq(&self, other: &G) -> bool {
        multi_polygon_eq(self, other)
    }
}
