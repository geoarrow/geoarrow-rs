use crate::algorithm::native::eq::multi_polygon_eq;
use crate::array::{CoordBuffer, MultiPolygonArray};
use crate::geo_traits::MultiPolygonTrait;
use crate::scalar::{MultiPolygon, Polygon};
use arrow_array::OffsetSizeTrait;
use arrow_buffer::OffsetBuffer;

#[derive(Clone, Debug)]
pub struct OwnedMultiPolygon<O: OffsetSizeTrait, const D: usize> {
    coords: CoordBuffer<D>,

    /// Offsets into the coordinate array where each geometry starts
    geom_offsets: OffsetBuffer<O>,

    polygon_offsets: OffsetBuffer<O>,

    ring_offsets: OffsetBuffer<O>,

    geom_index: usize,
}

impl<O: OffsetSizeTrait, const D: usize> OwnedMultiPolygon<O, D> {
    pub fn new(
        coords: CoordBuffer<D>,
        geom_offsets: OffsetBuffer<O>,
        polygon_offsets: OffsetBuffer<O>,
        ring_offsets: OffsetBuffer<O>,
        geom_index: usize,
    ) -> Self {
        Self {
            coords,
            geom_offsets,
            polygon_offsets,
            ring_offsets,
            geom_index,
        }
    }
}

impl<'a, O: OffsetSizeTrait, const D: usize> From<OwnedMultiPolygon<O, D>>
    for MultiPolygon<'a, O, D>
{
    fn from(value: OwnedMultiPolygon<O, D>) -> Self {
        Self::new_owned(
            value.coords,
            value.geom_offsets,
            value.polygon_offsets,
            value.ring_offsets,
            value.geom_index,
        )
    }
}

impl<'a, O: OffsetSizeTrait, const D: usize> From<&'a OwnedMultiPolygon<O, D>>
    for MultiPolygon<'a, O, D>
{
    fn from(value: &'a OwnedMultiPolygon<O, D>) -> Self {
        Self::new_borrowed(
            &value.coords,
            &value.geom_offsets,
            &value.polygon_offsets,
            &value.ring_offsets,
            value.geom_index,
        )
    }
}

impl<O: OffsetSizeTrait> From<OwnedMultiPolygon<O, 2>> for geo::MultiPolygon {
    fn from(value: OwnedMultiPolygon<O, 2>) -> Self {
        let geom = MultiPolygon::from(value);
        geom.into()
    }
}

impl<'a, O: OffsetSizeTrait, const D: usize> From<MultiPolygon<'a, O, D>>
    for OwnedMultiPolygon<O, D>
{
    fn from(value: MultiPolygon<'a, O, D>) -> Self {
        let (coords, geom_offsets, polygon_offsets, ring_offsets, geom_index) =
            value.into_owned_inner();
        Self::new(
            coords,
            geom_offsets,
            polygon_offsets,
            ring_offsets,
            geom_index,
        )
    }
}

impl<O: OffsetSizeTrait, const D: usize> From<OwnedMultiPolygon<O, D>> for MultiPolygonArray<O, D> {
    fn from(value: OwnedMultiPolygon<O, D>) -> Self {
        Self::new(
            value.coords,
            value.geom_offsets,
            value.polygon_offsets,
            value.ring_offsets,
            None,
            Default::default(),
        )
    }
}

impl<O: OffsetSizeTrait> MultiPolygonTrait for OwnedMultiPolygon<O, 2> {
    type T = f64;
    type ItemType<'b> = Polygon<'b, O, 2> where Self: 'b;

    fn dim(&self) -> usize {
        2
    }

    fn num_polygons(&self) -> usize {
        MultiPolygon::from(self).num_polygons()
    }

    unsafe fn polygon_unchecked(&self, i: usize) -> Self::ItemType<'_> {
        MultiPolygon::from(self).polygon_unchecked(i)
    }
}

impl<O: OffsetSizeTrait, G: MultiPolygonTrait<T = f64>> PartialEq<G> for OwnedMultiPolygon<O, 2> {
    fn eq(&self, other: &G) -> bool {
        multi_polygon_eq(self, other)
    }
}
