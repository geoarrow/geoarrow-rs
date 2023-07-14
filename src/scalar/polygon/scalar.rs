use crate::algorithm::native::bounding_rect::bounding_rect_polygon;
use crate::array::polygon::iterator::PolygonInteriorIterator;
use crate::array::polygon::parse_polygon;
use crate::array::CoordBuffer;
use crate::geo_traits::PolygonTrait;
use crate::scalar::LineString;
use arrow2::offset::OffsetsBuffer;
use arrow2::types::Offset;
use rstar::{RTreeObject, AABB};

// use super::iterator::PolygonInteriorIterator;

/// An Arrow equivalent of a Polygon
#[derive(Debug, Clone)]
pub struct Polygon<'a, O: Offset> {
    pub coords: &'a CoordBuffer,

    /// Offsets into the ring array where each geometry starts
    pub geom_offsets: &'a OffsetsBuffer<O>,

    /// Offsets into the coordinate array where each ring starts
    pub ring_offsets: &'a OffsetsBuffer<O>,

    pub geom_index: usize,
}

impl<'a, O: Offset> PolygonTrait<'a> for Polygon<'a, O> {
    type ItemType = LineString<'a, O>;
    type Iter = PolygonInteriorIterator<'a>;

    fn exterior(&'a self) -> Self::ItemType {
        let (start, _) = self.geom_offsets.start_end(self.geom_index);
        LineString {
            coords: self.coords,
            geom_offsets: self.ring_offsets,
            geom_index: start,
        }
    }

    fn interiors(&'a self) -> Self::Iter {
        PolygonInteriorIterator::new(self)
    }

    fn num_interiors(&'a self) -> usize {
        let (start, end) = self.geom_offsets.start_end(self.geom_index);
        end - start - 1
    }

    fn interior(&'a self, i: usize) -> Option<Self::ItemType> {
        let (start, end) = self.geom_offsets.start_end(self.geom_index);
        if i > (end - start - 1) {
            return None;
        }

        Some(LineString {
            coords: self.coords,
            geom_offsets: self.ring_offsets,
            geom_index: start + 1 + i,
        })
    }
}

impl<O: Offset> From<Polygon<'_, O>> for geo::Polygon {
    fn from(value: Polygon<'_, O>) -> Self {
        (&value).into()
    }
}

impl<O: Offset> From<&Polygon<'_, O>> for geo::Polygon {
    fn from(value: &Polygon<'_, O>) -> Self {
        parse_polygon(
            value.coords,
            value.geom_offsets,
            value.ring_offsets,
            value.geom_index,
        )
    }
}

impl<O: Offset> From<Polygon<'_, O>> for geo::Geometry {
    fn from(value: Polygon<'_, O>) -> Self {
        geo::Geometry::Polygon(value.into())
    }
}

impl<O: Offset> RTreeObject for Polygon<'_, O> {
    type Envelope = AABB<[f64; 2]>;

    fn envelope(&self) -> Self::Envelope {
        let (lower, upper) = bounding_rect_polygon(self);
        AABB::from_corners(lower, upper)
    }
}
