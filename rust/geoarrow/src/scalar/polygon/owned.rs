use crate::algorithm::native::eq::polygon_eq;
use crate::array::{CoordBuffer, PolygonArray};
use crate::datatypes::Dimension;
use crate::scalar::{LineString, Polygon};
use arrow_buffer::OffsetBuffer;
use geo_traits::PolygonTrait;

#[derive(Clone, Debug)]
pub struct OwnedPolygon {
    coords: CoordBuffer,

    /// Offsets into the coordinate array where each geometry starts
    geom_offsets: OffsetBuffer<i32>,

    ring_offsets: OffsetBuffer<i32>,

    geom_index: usize,
}

impl OwnedPolygon<D> {
    pub fn new(
        coords: CoordBuffer,
        geom_offsets: OffsetBuffer<i32>,
        ring_offsets: OffsetBuffer<i32>,
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

impl<'a> From<&'a OwnedPolygon<D>> for Polygon<'a> {
    fn from(value: &'a OwnedPolygon<D>) -> Self {
        Self::new(
            &value.coords,
            &value.geom_offsets,
            &value.ring_offsets,
            value.geom_index,
        )
    }
}

impl From<OwnedPolygon<2>> for geo::Polygon {
    fn from(value: OwnedPolygon<2>) -> Self {
        let geom = Polygon::from(&value);
        geom.into()
    }
}

impl<'a> From<Polygon<'a>> for OwnedPolygon<D> {
    fn from(value: Polygon<'a>) -> Self {
        let (coords, geom_offsets, ring_offsets, geom_index) = value.into_owned_inner();
        Self::new(coords, geom_offsets, ring_offsets, geom_index)
    }
}

impl From<OwnedPolygon<D>> for PolygonArray<D> {
    fn from(value: OwnedPolygon<D>) -> Self {
        Self::new(
            value.coords,
            value.geom_offsets,
            value.ring_offsets,
            None,
            Default::default(),
        )
    }
}

impl PolygonTrait for OwnedPolygon<D> {
    type T = f64;
    type RingType<'b> = LineString<'b,  D> where Self: 'b;

    fn dim(&self) -> geo_traits::Dimensions {
        match self.coords.dim() {
            Dimension::XY => geo_traits::Dimensions::Xy,
            Dimension::XYZ => geo_traits::Dimensions::Xyz,
        }
    }

    fn exterior(&self) -> Option<Self::RingType<'_>> {
        Polygon::from(self).exterior()
    }

    fn num_interiors(&self) -> usize {
        Polygon::from(self).num_interiors()
    }

    unsafe fn interior_unchecked(&self, i: usize) -> Self::RingType<'_> {
        Polygon::from(self).interior_unchecked(i)
    }
}

impl<G: PolygonTrait<T = f64>> PartialEq<G> for OwnedPolygon<2> {
    fn eq(&self, other: &G) -> bool {
        polygon_eq(self, other)
    }
}
