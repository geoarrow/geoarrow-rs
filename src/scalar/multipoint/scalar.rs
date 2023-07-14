use crate::algorithm::native::bounding_rect::bounding_rect_multipoint;
use crate::array::multipoint::MultiPointIterator;
use crate::array::CoordBuffer;
use crate::geo_traits::MultiPointTrait;
use crate::scalar::Point;
use crate::GeometryArrayTrait;
use arrow2::offset::OffsetsBuffer;
use arrow2::types::Offset;
use rstar::{RTreeObject, AABB};

/// An Arrow equivalent of a MultiPoint
#[derive(Debug, Clone)]
pub struct MultiPoint<'a, O: Offset> {
    /// Buffer of coordinates
    pub coords: &'a CoordBuffer,

    /// Offsets into the coordinate array where each geometry starts
    pub geom_offsets: &'a OffsetsBuffer<O>,

    pub geom_index: usize,
}

impl<'a, O: Offset> MultiPointTrait<'a> for MultiPoint<'a, O> {
    type ItemType = Point<'a>;
    type Iter = MultiPointIterator<'a>;

    fn points(&'a self) -> Self::Iter {
        MultiPointIterator::new(self)
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

impl<O: Offset> From<MultiPoint<'_, O>> for geo::MultiPoint {
    fn from(value: MultiPoint<'_, O>) -> Self {
        (&value).into()
    }
}

impl<O: Offset> From<&MultiPoint<'_, O>> for geo::MultiPoint {
    fn from(value: &MultiPoint<'_, O>) -> Self {
        let (start_idx, end_idx) = value.geom_offsets.start_end(value.geom_index);
        let mut coords: Vec<geo::Point> = Vec::with_capacity(end_idx - start_idx);

        for i in start_idx..end_idx {
            coords.push(value.coords.value(i).into());
        }

        geo::MultiPoint::new(coords)
    }
}

impl<O: Offset> From<MultiPoint<'_, O>> for geo::Geometry {
    fn from(value: MultiPoint<'_, O>) -> Self {
        geo::Geometry::MultiPoint(value.into())
    }
}

impl<O: Offset> RTreeObject for MultiPoint<'_, O> {
    type Envelope = AABB<[f64; 2]>;

    fn envelope(&self) -> Self::Envelope {
        let (lower, upper) = bounding_rect_multipoint(self);
        AABB::from_corners(lower, upper)
    }
}
