use crate::algorithm::native::eq::polygon_eq;
use crate::array::{CoordBuffer, PolygonArray};
use crate::geo_traits::PolygonTrait;
use crate::scalar::{LineString, Polygon};
use arrow_buffer::OffsetBuffer;

#[derive(Clone, Debug)]
pub struct OwnedPolygon<const D: usize> {
    coords: CoordBuffer<D>,

    /// Offsets into the coordinate array where each geometry starts
    geom_offsets: OffsetBuffer<i32>,

    ring_offsets: OffsetBuffer<i32>,

    geom_index: usize,
}

impl<const D: usize> OwnedPolygon<D> {
    pub fn new(coords: CoordBuffer<D>, geom_offsets: OffsetBuffer<i32>, ring_offsets: OffsetBuffer<i32>, geom_index: usize) -> Self {
        Self { coords, geom_offsets, ring_offsets, geom_index }
    }
}

impl<'a, const D: usize> From<&'a OwnedPolygon<D>> for Polygon<'a, D> {
    fn from(value: &'a OwnedPolygon<D>) -> Self {
        Self::new(&value.coords, &value.geom_offsets, &value.ring_offsets, value.geom_index)
    }
}

impl From<OwnedPolygon<2>> for geo::Polygon {
    fn from(value: OwnedPolygon<2>) -> Self {
        let geom = Polygon::from(&value);
        geom.into()
    }
}

impl<'a, const D: usize> From<Polygon<'a, D>> for OwnedPolygon<D> {
    fn from(value: Polygon<'a, D>) -> Self {
        let (coords, geom_offsets, ring_offsets, geom_index) = value.into_owned_inner();
        Self::new(coords, geom_offsets, ring_offsets, geom_index)
    }
}

impl<const D: usize> From<OwnedPolygon<D>> for PolygonArray<D> {
    fn from(value: OwnedPolygon<D>) -> Self {
        Self::new(value.coords, value.geom_offsets, value.ring_offsets, None, Default::default())
    }
}

impl<const D: usize> PolygonTrait for OwnedPolygon<D> {
    type T = f64;
    type ItemType<'b> = LineString<'b,  D> where Self: 'b;

    fn dim(&self) -> usize {
        D
    }

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

impl<G: PolygonTrait<T = f64>> PartialEq<G> for OwnedPolygon<2> {
    fn eq(&self, other: &G) -> bool {
        polygon_eq(self, other)
    }
}
