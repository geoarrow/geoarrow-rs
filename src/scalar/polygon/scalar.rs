use crate::algorithm::native::bounding_rect::bounding_rect_polygon;
use crate::array::polygon::iterator::PolygonInteriorIterator;
use crate::array::polygon::parse_polygon;
use crate::array::CoordBuffer;
use crate::geo_traits::PolygonTrait;
use crate::scalar::LineString;
use crate::trait_::GeometryScalarTrait;
use arrow2::offset::OffsetsBuffer;
use arrow2::types::Offset;
use rstar::{RTreeObject, AABB};

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

impl<'a, O: Offset> GeometryScalarTrait<'a> for Polygon<'a, O> {
    type ScalarGeo = geo::Polygon;

    fn to_geo(&self) -> Self::ScalarGeo {
        self.into()
    }
}

impl<'a, O: Offset> PolygonTrait<'a> for Polygon<'a, O> {
    type T = f64;
    type ItemType = LineString<'a, O>;
    type Iter = PolygonInteriorIterator<'a, O>;

    fn exterior(&self) -> Self::ItemType {
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

    fn num_interiors(&self) -> usize {
        let (start, end) = self.geom_offsets.start_end(self.geom_index);
        end - start - 1
    }

    fn interior(&self, i: usize) -> Option<Self::ItemType> {
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

impl<'a, O: Offset> PolygonTrait<'a> for &Polygon<'a, O> {
    type T = f64;
    type ItemType = LineString<'a, O>;
    type Iter = PolygonInteriorIterator<'a, O>;

    fn exterior(&self) -> Self::ItemType {
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

    fn num_interiors(&self) -> usize {
        let (start, end) = self.geom_offsets.start_end(self.geom_index);
        end - start - 1
    }

    fn interior(&self, i: usize) -> Option<Self::ItemType> {
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

impl<O: Offset> PartialEq for Polygon<'_, O> {
    fn eq(&self, other: &Self) -> bool {
        // TODO: there's probably a way to use the underlying arrays for equality directly, instead
        // of going through the trait API, but that takes a little more thought to implement
        // correctly. In particular, you can't just check whether the coord arrays are equal; you
        // also need to make sure the ring start and ends are equal. But the ring offsets may be
        // different
        if self.num_interiors() != other.num_interiors() {
            return false;
        }

        if self.exterior() != other.exterior() {
            return false;
        }

        for i in 0..self.num_interiors() {
            if self.interior(i) != other.interior(i) {
                return false;
            }
        }

        true
    }
}

#[cfg(test)]
mod test {
    use crate::array::PolygonArray;
    use crate::test::polygon::{p0, p1};
    use crate::GeometryArrayTrait;

    /// Test Eq where the current index is true but another index is false
    #[test]
    fn test_eq_other_index_false() {
        let arr1: PolygonArray<i32> = vec![p0(), p1()].into();
        let arr2: PolygonArray<i32> = vec![p0(), p0()].into();

        assert_eq!(arr1.value(0), arr2.value(0));
        assert_ne!(arr1.value(1), arr2.value(1));
    }
}
