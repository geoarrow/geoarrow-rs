use crate::algorithm::native::bounding_rect::bounding_rect_linestring;
use crate::array::CoordBuffer;
use crate::geo_traits::LineStringTrait;
use crate::scalar::Point;
use arrow2::offset::OffsetsBuffer;
use arrow2::types::Offset;
use rstar::{RTreeObject, AABB};

use crate::array::linestring::LineStringIterator;

/// An Arrow equivalent of a LineString
#[derive(Debug, Clone)]
pub struct LineString<'a, O: Offset> {
    pub coords: &'a CoordBuffer,

    /// Offsets into the coordinate array where each geometry starts
    pub geom_offsets: &'a OffsetsBuffer<O>,

    pub geom_index: usize,
}

impl<'a, O: Offset> LineStringTrait<'a> for LineString<'a, O> {
    type ItemType = Point<'a>;
    type Iter = LineStringIterator<'a, O>;

    fn points(&'a self) -> Self::Iter {
        LineStringIterator::new(self)
    }

    fn num_points(&self) -> usize {
        let (start, end) = self.geom_offsets.start_end(self.geom_index);
        end - start
    }

    fn point(&'a self, i: usize) -> Option<Self::ItemType> {
        let (start, end) = self.geom_offsets.start_end(self.geom_index);
        if i > (end - start) {
            return None;
        }

        let point = Point {
            coords: self.coords,
            geom_index: start + i,
        };
        Some(point)
    }
}

impl<O: Offset> From<LineString<'_, O>> for geo::LineString {
    fn from(value: LineString<'_, O>) -> Self {
        (&value).into()
    }
}

impl<O: Offset> From<&LineString<'_, O>> for geo::LineString {
    fn from(value: &LineString<'_, O>) -> Self {
        let num_coords = value.num_points();
        let mut coords: Vec<geo::Coord> = Vec::with_capacity(num_coords);

        for i in 0..num_coords {
            coords.push(value.point(i).unwrap().into());
        }

        geo::LineString::new(coords)
    }
}

impl<O: Offset> From<LineString<'_, O>> for geo::Geometry {
    fn from(value: LineString<'_, O>) -> Self {
        geo::Geometry::LineString(value.into())
    }
}

impl<O: Offset> RTreeObject for LineString<'_, O> {
    type Envelope = AABB<[f64; 2]>;

    fn envelope(&self) -> Self::Envelope {
        let (lower, upper) = bounding_rect_linestring(self);
        AABB::from_corners(lower, upper)
    }
}
