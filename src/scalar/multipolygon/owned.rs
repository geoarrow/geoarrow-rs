use crate::algorithm::native::eq::multi_polygon_eq;
use crate::array::CoordBuffer;
use crate::geo_traits::MultiPolygonTrait;
use crate::scalar::{MultiPolygon, Polygon};
use arrow_array::OffsetSizeTrait;
use arrow_buffer::OffsetBuffer;

#[derive(Debug)]
pub struct OwnedMultiPolygon<O: OffsetSizeTrait> {
    coords: CoordBuffer,

    /// Offsets into the coordinate array where each geometry starts
    geom_offsets: OffsetBuffer<O>,

    polygon_offsets: OffsetBuffer<O>,

    ring_offsets: OffsetBuffer<O>,

    geom_index: usize,
}

impl<O: OffsetSizeTrait> OwnedMultiPolygon<O> {
    pub fn new(
        coords: CoordBuffer,
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

impl<'a, O: OffsetSizeTrait> From<OwnedMultiPolygon<O>> for MultiPolygon<'a, O> {
    fn from(value: OwnedMultiPolygon<O>) -> Self {
        Self::new_owned(
            value.coords,
            value.geom_offsets,
            value.polygon_offsets,
            value.ring_offsets,
            value.geom_index,
        )
    }
}

impl<'a, O: OffsetSizeTrait> From<&'a OwnedMultiPolygon<O>> for MultiPolygon<'a, O> {
    fn from(value: &'a OwnedMultiPolygon<O>) -> Self {
        Self::new_borrowed(
            &value.coords,
            &value.geom_offsets,
            &value.polygon_offsets,
            &value.ring_offsets,
            value.geom_index,
        )
    }
}

impl<O: OffsetSizeTrait> From<OwnedMultiPolygon<O>> for geo::MultiPolygon {
    fn from(value: OwnedMultiPolygon<O>) -> Self {
        let geom = MultiPolygon::from(value);
        geom.into()
    }
}

impl<'a, O: OffsetSizeTrait> From<MultiPolygon<'a, O>> for OwnedMultiPolygon<O> {
    fn from(value: MultiPolygon<'a, O>) -> Self {
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

impl<O: OffsetSizeTrait> MultiPolygonTrait for OwnedMultiPolygon<O> {
    type T = f64;
    type ItemType<'b> = Polygon<'b, O> where Self: 'b;

    fn num_polygons(&self) -> usize {
        MultiPolygon::from(self).num_polygons()
    }

    unsafe fn polygon_unchecked(&self, i: usize) -> Self::ItemType<'_> {
        MultiPolygon::from(self).polygon_unchecked(i)
    }
}

impl<O: OffsetSizeTrait, G: MultiPolygonTrait<T = f64>> PartialEq<G> for OwnedMultiPolygon<O> {
    fn eq(&self, other: &G) -> bool {
        multi_polygon_eq(self, other)
    }
}
