use crate::algorithm::native::eq::polygon_eq;
use crate::array::{CoordBuffer, PolygonArray};
use crate::geo_traits::PolygonTrait;
use crate::scalar::{LineString, Polygon};
use arrow_array::OffsetSizeTrait;
use arrow_buffer::OffsetBuffer;

#[derive(Clone, Debug)]
pub struct OwnedPolygon<O: OffsetSizeTrait, const D: usize> {
    coords: CoordBuffer<D>,

    /// Offsets into the coordinate array where each geometry starts
    geom_offsets: OffsetBuffer<O>,

    ring_offsets: OffsetBuffer<O>,

    geom_index: usize,
}

impl<O: OffsetSizeTrait, const D: usize> OwnedPolygon<O, D> {
    pub fn new(
        coords: CoordBuffer<D>,
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

impl<'a, O: OffsetSizeTrait, const D: usize> From<OwnedPolygon<O, D>> for Polygon<'a, O, D> {
    fn from(value: OwnedPolygon<O, D>) -> Self {
        Self::new_owned(
            value.coords,
            value.geom_offsets,
            value.ring_offsets,
            value.geom_index,
        )
    }
}

impl<'a, O: OffsetSizeTrait, const D: usize> From<&'a OwnedPolygon<O, D>> for Polygon<'a, O, D> {
    fn from(value: &'a OwnedPolygon<O, D>) -> Self {
        Self::new_borrowed(
            &value.coords,
            &value.geom_offsets,
            &value.ring_offsets,
            value.geom_index,
        )
    }
}

impl<O: OffsetSizeTrait> From<OwnedPolygon<O, 2>> for geo::Polygon {
    fn from(value: OwnedPolygon<O, 2>) -> Self {
        let geom = Polygon::from(value);
        geom.into()
    }
}

impl<'a, O: OffsetSizeTrait, const D: usize> From<Polygon<'a, O, D>> for OwnedPolygon<O, D> {
    fn from(value: Polygon<'a, O, D>) -> Self {
        let (coords, geom_offsets, ring_offsets, geom_index) = value.into_owned_inner();
        Self::new(coords, geom_offsets, ring_offsets, geom_index)
    }
}

impl<O: OffsetSizeTrait, const D: usize> From<OwnedPolygon<O, D>> for PolygonArray<O, D> {
    fn from(value: OwnedPolygon<O, D>) -> Self {
        Self::new(
            value.coords,
            value.geom_offsets,
            value.ring_offsets,
            None,
            Default::default(),
        )
    }
}

impl<O: OffsetSizeTrait> PolygonTrait for OwnedPolygon<O, 2> {
    type T = f64;
    type ItemType<'b> = LineString<'b, O, 2> where Self: 'b;

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

impl<O: OffsetSizeTrait, G: PolygonTrait<T = f64>> PartialEq<G> for OwnedPolygon<O, 2> {
    fn eq(&self, other: &G) -> bool {
        polygon_eq(self, other)
    }
}
