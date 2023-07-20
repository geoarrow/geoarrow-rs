use crate::algorithm::native::bounding_rect::bounding_rect_multipolygon;
use crate::array::multipolygon::MultiPolygonIterator;
use crate::array::CoordBuffer;
use crate::geo_traits::MultiPolygonTrait;
use crate::scalar::Polygon;
use crate::trait_::GeometryScalarTrait;
use arrow2::offset::OffsetsBuffer;
use arrow2::types::Offset;
use rstar::{RTreeObject, AABB};

/// An Arrow equivalent of a MultiPolygon
#[derive(Debug, Clone, PartialEq)]
pub struct MultiPolygon<'a, O: Offset> {
    pub coords: &'a CoordBuffer,

    /// Offsets into the polygon array where each geometry starts
    pub geom_offsets: &'a OffsetsBuffer<O>,

    /// Offsets into the ring array where each polygon starts
    pub polygon_offsets: &'a OffsetsBuffer<O>,

    /// Offsets into the coordinate array where each ring starts
    pub ring_offsets: &'a OffsetsBuffer<O>,

    pub geom_index: usize,
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
        Some(Polygon {
            coords: self.coords,
            geom_offsets: self.polygon_offsets,
            ring_offsets: self.ring_offsets,
            geom_index: start + i,
        })
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
                value.coords,
                value.polygon_offsets,
                value.ring_offsets,
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
