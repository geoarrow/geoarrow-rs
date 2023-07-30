use crate::algorithm::native::bounding_rect::bounding_rect_multipolygon;
use crate::array::multipolygon::MultiPolygonIterator;
use crate::array::CoordBuffer;
use crate::geo_traits::MultiPolygonTrait;
use crate::scalar::Polygon;
use crate::trait_::GeometryScalarTrait;
use arrow2::offset::OffsetsBuffer;
use arrow2::types::Offset;
use rstar::{RTreeObject, AABB};
use std::borrow::Cow;

/// An Arrow equivalent of a MultiPolygon
#[derive(Debug, Clone)]
pub struct MultiPolygon<'a, O: Offset> {
    pub coords: Cow<'a, CoordBuffer>,

    /// Offsets into the polygon array where each geometry starts
    pub geom_offsets: Cow<'a, OffsetsBuffer<O>>,

    /// Offsets into the ring array where each polygon starts
    pub polygon_offsets: Cow<'a, OffsetsBuffer<O>>,

    /// Offsets into the coordinate array where each ring starts
    pub ring_offsets: Cow<'a, OffsetsBuffer<O>>,

    pub geom_index: usize,
}

impl<'a, O: Offset> MultiPolygon<'a, O> {
    pub fn new(
        coords: Cow<'a, CoordBuffer>,
        geom_offsets: Cow<'a, OffsetsBuffer<O>>,
        polygon_offsets: Cow<'a, OffsetsBuffer<O>>,
        ring_offsets: Cow<'a, OffsetsBuffer<O>>,
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

    pub fn new_borrowed(
        coords: &'a CoordBuffer,
        geom_offsets: &'a OffsetsBuffer<O>,
        polygon_offsets: &'a OffsetsBuffer<O>,
        ring_offsets: &'a OffsetsBuffer<O>,
        geom_index: usize,
    ) -> Self {
        Self {
            coords: Cow::Borrowed(coords),
            geom_offsets: Cow::Borrowed(geom_offsets),
            polygon_offsets: Cow::Borrowed(polygon_offsets),
            ring_offsets: Cow::Borrowed(ring_offsets),
            geom_index,
        }
    }

    pub fn new_owned(
        coords: CoordBuffer,
        geom_offsets: OffsetsBuffer<O>,
        polygon_offsets: OffsetsBuffer<O>,
        ring_offsets: OffsetsBuffer<O>,
        geom_index: usize,
    ) -> Self {
        Self {
            coords: Cow::Owned(coords),
            geom_offsets: Cow::Owned(geom_offsets),
            polygon_offsets: Cow::Owned(polygon_offsets),
            ring_offsets: Cow::Owned(ring_offsets),
            geom_index,
        }
    }
}

impl<'a, O: Offset> GeometryScalarTrait<'a> for MultiPolygon<'a, O> {
    type ScalarGeo = geo::MultiPolygon;

    fn to_geo(&self) -> Self::ScalarGeo {
        self.into()
    }
}

impl<'a, O: Offset> MultiPolygonTrait<'a> for MultiPolygon<'a, O> {
    type T = f64;
    type ItemType = Polygon<'a, O>;
    type Iter = MultiPolygonIterator<'a, O>;

    fn polygons(&'a self) -> Self::Iter {
        MultiPolygonIterator::new(self)
    }

    fn num_polygons(&self) -> usize {
        let (start, end) = self.geom_offsets.start_end(self.geom_index);
        end - start
    }

    fn polygon(&self, i: usize) -> Option<Self::ItemType> {
        let (start, end) = self.geom_offsets.start_end(self.geom_index);
        if i > (end - start) {
            return None;
        }

        // TODO: double check offsets is correct
        Some(Polygon::new(
            self.coords.clone(),
            self.polygon_offsets.clone(),
            self.ring_offsets.clone(),
            start + i,
        ))
    }
}

impl<O: Offset> From<MultiPolygon<'_, O>> for geo::MultiPolygon {
    fn from(value: MultiPolygon<'_, O>) -> Self {
        (&value).into()
    }
}

impl<O: Offset> From<&MultiPolygon<'_, O>> for geo::MultiPolygon {
    fn from(value: &MultiPolygon<'_, O>) -> Self {
        // Start and end indices into the polygon_offsets buffer
        let (start_geom_idx, end_geom_idx) = value.geom_offsets.start_end(value.geom_index);

        let mut polygons: Vec<geo::Polygon> = Vec::with_capacity(end_geom_idx - start_geom_idx);

        for geom_idx in start_geom_idx..end_geom_idx {
            let poly = crate::array::polygon::util::parse_polygon(
                value.coords.clone(),
                value.polygon_offsets.clone(),
                value.ring_offsets.clone(),
                geom_idx,
            );
            polygons.push(poly);
        }

        geo::MultiPolygon::new(polygons)
    }
}

impl<O: Offset> From<MultiPolygon<'_, O>> for geo::Geometry {
    fn from(value: MultiPolygon<'_, O>) -> Self {
        geo::Geometry::MultiPolygon(value.into())
    }
}

impl<O: Offset> RTreeObject for MultiPolygon<'_, O> {
    type Envelope = AABB<[f64; 2]>;

    fn envelope(&self) -> Self::Envelope {
        let (lower, upper) = bounding_rect_multipolygon(self);
        AABB::from_corners(lower, upper)
    }
}

impl<O: Offset> PartialEq for MultiPolygon<'_, O> {
    fn eq(&self, other: &Self) -> bool {
        if self.num_polygons() != other.num_polygons() {
            return false;
        }

        for i in 0..self.num_polygons() {
            if self.polygon(i) != other.polygon(i) {
                return false;
            }
        }

        true
    }
}

#[cfg(test)]
mod test {
    use crate::array::MultiPolygonArray;
    use crate::test::multipolygon::{mp0, mp1};
    use crate::GeometryArrayTrait;

    /// Test Eq where the current index is true but another index is false
    #[test]
    fn test_eq_other_index_false() {
        let arr1: MultiPolygonArray<i32> = vec![mp0(), mp1()].into();
        let arr2: MultiPolygonArray<i32> = vec![mp0(), mp0()].into();

        assert_eq!(arr1.value(0), arr2.value(0));
        assert_ne!(arr1.value(1), arr2.value(1));
    }
}
