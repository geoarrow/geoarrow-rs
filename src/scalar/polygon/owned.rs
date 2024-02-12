use crate::algorithm::native::eq::polygon_eq;
use crate::array::CoordBuffer;
use crate::geo_traits::PolygonTrait;
use crate::scalar::{LineString, Polygon};
use arrow_array::OffsetSizeTrait;
use arrow_buffer::OffsetBuffer;

#[derive(Debug)]
pub struct OwnedPolygon<O: OffsetSizeTrait> {
    coords: CoordBuffer,

    /// Offsets into the coordinate array where each geometry starts
    geom_offsets: OffsetBuffer<O>,

    ring_offsets: OffsetBuffer<O>,

    geom_index: usize,
}

impl<O: OffsetSizeTrait> OwnedPolygon<O> {
    pub fn new(
        coords: CoordBuffer,
        geom_offsets: OffsetBuffer<O>,
        ring_offsets: OffsetBuffer<O>,
        geom_index: usize,
    ) -> Self {
        Self {
            coords,
            geom_offsets,
            ring_offsets,
            geom_index,
        }
    }
}

impl<'a, O: OffsetSizeTrait> From<OwnedPolygon<O>> for Polygon<'a, O> {
    fn from(value: OwnedPolygon<O>) -> Self {
        Self::new_owned(
            value.coords,
            value.geom_offsets,
            value.ring_offsets,
            value.geom_index,
        )
    }
}

impl<'a, O: OffsetSizeTrait> From<&'a OwnedPolygon<O>> for Polygon<'a, O> {
    fn from(value: &'a OwnedPolygon<O>) -> Self {
        Self::new_borrowed(
            &value.coords,
            &value.geom_offsets,
            &value.ring_offsets,
            value.geom_index,
        )
    }
}

impl<O: OffsetSizeTrait> From<OwnedPolygon<O>> for geo::Polygon {
    fn from(value: OwnedPolygon<O>) -> Self {
        let geom = Polygon::from(value);
        geom.into()
    }
}

impl<'a, O: OffsetSizeTrait> From<Polygon<'a, O>> for OwnedPolygon<O> {
    fn from(value: Polygon<'a, O>) -> Self {
        let (coords, geom_offsets, ring_offsets, geom_index) = value.into_owned_inner();
        Self::new(coords, geom_offsets, ring_offsets, geom_index)
    }
}

impl<O: OffsetSizeTrait> PolygonTrait for OwnedPolygon<O> {
    type T = f64;
    type ItemType<'b> = LineString<'b, O> where Self: 'b;

    fn exterior(&self) -> Option<Self::ItemType<'_>> {
        Polygon::from(self).exterior()
    }

    fn num_interiors(&self) -> usize {
        Polygon::from(self).num_interiors()
    }

    unsafe fn interior_unchecked(&self, i: usize) -> Self::ItemType<'_> {
        Polygon::from(self).interior_unchecked(i)
    }
}

impl<O: OffsetSizeTrait, G: PolygonTrait<T = f64>> PartialEq<G> for OwnedPolygon<O> {
    fn eq(&self, other: &G) -> bool {
        polygon_eq(self, other)
    }
}
