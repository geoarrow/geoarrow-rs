use crate::algorithm::native::bounding_rect::bounding_rect_multilinestring;
use crate::array::multilinestring::MultiLineStringIterator;
use crate::array::CoordBuffer;
use crate::geo_traits::MultiLineStringTrait;
use crate::scalar::LineString;
use crate::GeometryArrayTrait;
use arrow2::offset::OffsetsBuffer;
use arrow2::types::Offset;
use rstar::{RTreeObject, AABB};

/// An Arrow equivalent of a Polygon
#[derive(Debug, Clone, PartialEq)]
pub struct MultiLineString<'a, O: Offset> {
    pub coords: &'a CoordBuffer,

    /// Offsets into the ring array where each geometry starts
    pub geom_offsets: &'a OffsetsBuffer<O>,

    /// Offsets into the coordinate array where each ring starts
    pub ring_offsets: &'a OffsetsBuffer<O>,

    pub geom_index: usize,
}

impl<'a, O: Offset> MultiLineStringTrait<'a> for MultiLineString<'a, O> {
    type T = f64;
    type ItemType = LineString<'a, O>;
    type Iter = MultiLineStringIterator<'a, O>;

    fn lines(&'a self) -> Self::Iter {
        MultiLineStringIterator::new(self)
    }

    fn num_lines(&self) -> usize {
        let (start, end) = self.geom_offsets.start_end(self.geom_index);
        end - start
    }

    fn line(&self, i: usize) -> Option<Self::ItemType> {
        let (start, end) = self.geom_offsets.start_end(self.geom_index);
        if i > (end - start) {
            return None;
        }

        Some(LineString {
            coords: self.coords,
            geom_offsets: self.ring_offsets,
            geom_index: start + i,
        })
    }
}

impl<O: Offset> From<MultiLineString<'_, O>> for geo::MultiLineString {
    fn from(value: MultiLineString<'_, O>) -> Self {
        (&value).into()
    }
}

impl<O: Offset> From<&MultiLineString<'_, O>> for geo::MultiLineString {
    fn from(value: &MultiLineString<'_, O>) -> Self {
        // Start and end indices into the ring_offsets buffer
        let (start_geom_idx, end_geom_idx) = value.geom_offsets.start_end(value.geom_index);

        let mut line_strings: Vec<geo::LineString> =
            Vec::with_capacity(end_geom_idx - start_geom_idx);

        for ring_idx in start_geom_idx..end_geom_idx {
            let (start_coord_idx, end_coord_idx) = value.ring_offsets.start_end(ring_idx);
            let mut ring: Vec<geo::Coord> = Vec::with_capacity(end_coord_idx - start_coord_idx);
            for coord_idx in start_coord_idx..end_coord_idx {
                ring.push(value.coords.value(coord_idx).into())
            }
            line_strings.push(ring.into());
        }

        geo::MultiLineString::new(line_strings)
    }
}

impl<O: Offset> From<MultiLineString<'_, O>> for geo::Geometry {
    fn from(value: MultiLineString<'_, O>) -> Self {
        geo::Geometry::MultiLineString(value.into())
    }
}

impl<O: Offset> RTreeObject for MultiLineString<'_, O> {
    type Envelope = AABB<[f64; 2]>;

    fn envelope(&self) -> Self::Envelope {
        let (lower, upper) = bounding_rect_multilinestring(self);
        AABB::from_corners(lower, upper)
    }
}
