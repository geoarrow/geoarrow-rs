use crate::algorithm::native::eq::multi_polygon_eq;
use crate::array::{CoordBuffer, MultiPolygonArray};
use crate::datatypes::Dimension;
use crate::scalar::{MultiPolygon, Polygon};
use arrow_buffer::OffsetBuffer;
use geo_traits::MultiPolygonTrait;

#[derive(Clone, Debug)]
pub struct OwnedMultiPolygon {
    coords: CoordBuffer,

    /// Offsets into the coordinate array where each geometry starts
    geom_offsets: OffsetBuffer<i32>,

    polygon_offsets: OffsetBuffer<i32>,

    ring_offsets: OffsetBuffer<i32>,

    geom_index: usize,
}

impl OwnedMultiPolygon {
    pub fn new(
        coords: CoordBuffer,
        geom_offsets: OffsetBuffer<i32>,
        polygon_offsets: OffsetBuffer<i32>,
        ring_offsets: OffsetBuffer<i32>,
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

impl<'a> From<&'a OwnedMultiPolygon> for MultiPolygon<'a> {
    fn from(value: &'a OwnedMultiPolygon) -> Self {
        Self::new(
            &value.coords,
            &value.geom_offsets,
            &value.polygon_offsets,
            &value.ring_offsets,
            value.geom_index,
        )
    }
}

impl From<OwnedMultiPolygon> for geo::MultiPolygon {
    fn from(value: OwnedMultiPolygon) -> Self {
        let geom = MultiPolygon::from(&value);
        geom.into()
    }
}

impl<'a> From<MultiPolygon<'a>> for OwnedMultiPolygon {
    fn from(value: MultiPolygon<'a>) -> Self {
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

impl From<OwnedMultiPolygon> for MultiPolygonArray {
    fn from(value: OwnedMultiPolygon) -> Self {
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

impl MultiPolygonTrait for OwnedMultiPolygon {
    type T = f64;
    type PolygonType<'b> = Polygon<'b> where Self: 'b;

    fn dim(&self) -> geo_traits::Dimensions {
        match self.coords.dim() {
            Dimension::XY => geo_traits::Dimensions::Xy,
            Dimension::XYZ => geo_traits::Dimensions::Xyz,
        }
    }

    fn num_polygons(&self) -> usize {
        MultiPolygon::from(self).num_polygons()
    }

    unsafe fn polygon_unchecked(&self, i: usize) -> Self::PolygonType<'_> {
        MultiPolygon::from(self).polygon_unchecked(i)
    }
}

impl<G: MultiPolygonTrait<T = f64>> PartialEq<G> for OwnedMultiPolygon {
    fn eq(&self, other: &G) -> bool {
        multi_polygon_eq(self, other)
    }
}
