use crate::algorithm::native::eq::polygon_eq;
use crate::array::{CoordBuffer, PolygonArray};
use geoarrow_schema::Dimension;
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

impl OwnedPolygon {
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

impl<'a> From<&'a OwnedPolygon> for Polygon<'a> {
    fn from(value: &'a OwnedPolygon) -> Self {
        Self::new(
            &value.coords,
            &value.geom_offsets,
            &value.ring_offsets,
            value.geom_index,
        )
    }
}

impl From<OwnedPolygon> for geo::Polygon {
    fn from(value: OwnedPolygon) -> Self {
        let geom = Polygon::from(&value);
        geom.into()
    }
}

impl<'a> From<Polygon<'a>> for OwnedPolygon {
    fn from(value: Polygon<'a>) -> Self {
        let (coords, geom_offsets, ring_offsets, geom_index) = value.into_owned_inner();
        Self::new(coords, geom_offsets, ring_offsets, geom_index)
    }
}

impl From<OwnedPolygon> for PolygonArray {
    fn from(value: OwnedPolygon) -> Self {
        Self::new(
            value.coords,
            value.geom_offsets,
            value.ring_offsets,
            None,
            Default::default(),
        )
    }
}

impl PolygonTrait for OwnedPolygon {
    type T = f64;
    type RingType<'b>
        = LineString<'b>
    where
        Self: 'b;

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

impl<G: PolygonTrait<T = f64>> PartialEq<G> for OwnedPolygon {
    fn eq(&self, other: &G) -> bool {
        polygon_eq(self, other)
    }
}
