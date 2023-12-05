use crate::algorithm::native::bounding_rect::bounding_rect_multipolygon;
use crate::algorithm::native::eq::multi_polygon_eq;
use crate::array::util::OffsetBufferUtils;
use crate::array::{CoordBuffer, MultiPolygonArray};
use crate::geo_traits::MultiPolygonTrait;
use crate::scalar::multipolygon::MultiPolygonIterator;
use crate::scalar::Polygon;
use crate::trait_::{GeometryArraySelfMethods, GeometryScalarTrait};
use arrow_array::OffsetSizeTrait;
use arrow_buffer::OffsetBuffer;
use rstar::{RTreeObject, AABB};
use std::borrow::Cow;

/// An Arrow equivalent of a MultiPolygon
#[derive(Debug, Clone)]
pub struct MultiPolygon<'a, O: OffsetSizeTrait> {
    pub coords: Cow<'a, CoordBuffer>,

    /// Offsets into the polygon array where each geometry starts
    pub geom_offsets: Cow<'a, OffsetBuffer<O>>,

    /// Offsets into the ring array where each polygon starts
    pub polygon_offsets: Cow<'a, OffsetBuffer<O>>,

    /// Offsets into the coordinate array where each ring starts
    pub ring_offsets: Cow<'a, OffsetBuffer<O>>,

    pub geom_index: usize,
}

impl<'a, O: OffsetSizeTrait> MultiPolygon<'a, O> {
    pub fn new(
        coords: Cow<'a, CoordBuffer>,
        geom_offsets: Cow<'a, OffsetBuffer<O>>,
        polygon_offsets: Cow<'a, OffsetBuffer<O>>,
        ring_offsets: Cow<'a, OffsetBuffer<O>>,
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
        geom_offsets: &'a OffsetBuffer<O>,
        polygon_offsets: &'a OffsetBuffer<O>,
        ring_offsets: &'a OffsetBuffer<O>,
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
        geom_offsets: OffsetBuffer<O>,
        polygon_offsets: OffsetBuffer<O>,
        ring_offsets: OffsetBuffer<O>,
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

    /// Extracts the owned data.
    ///
    /// Clones the data if it is not already owned.
    pub fn into_owned(self) -> Self {
        let arr = MultiPolygonArray::new(
            self.coords.into_owned(),
            self.geom_offsets.into_owned(),
            self.polygon_offsets.into_owned(),
            self.ring_offsets.into_owned(),
            None,
        );
        let sliced_arr = arr.owned_slice(self.geom_index, 1);
        Self::new_owned(
            sliced_arr.coords,
            sliced_arr.geom_offsets,
            sliced_arr.polygon_offsets,
            sliced_arr.ring_offsets,
            0,
        )
    }

    pub fn into_owned_inner(
        self,
    ) -> (
        CoordBuffer,
        OffsetBuffer<O>,
        OffsetBuffer<O>,
        OffsetBuffer<O>,
        usize,
    ) {
        let owned = self.into_owned();
        (
            owned.coords.into_owned(),
            owned.geom_offsets.into_owned(),
            owned.polygon_offsets.into_owned(),
            owned.ring_offsets.into_owned(),
            owned.geom_index,
        )
    }
}

impl<'a, O: OffsetSizeTrait> GeometryScalarTrait for MultiPolygon<'a, O> {
    type ScalarGeo = geo::MultiPolygon;

    fn to_geo(&self) -> Self::ScalarGeo {
        self.into()
    }
}

impl<'a, O: OffsetSizeTrait> MultiPolygonTrait for MultiPolygon<'a, O> {
    type T = f64;
    type ItemType<'b> = Polygon<'a, O> where Self: 'b;
    type Iter<'b> = MultiPolygonIterator<'a, O> where Self: 'b;

    fn polygons(&self) -> Self::Iter<'_> {
        todo!()
        // MultiPolygonIterator::new(self)
    }

    fn num_polygons(&self) -> usize {
        let (start, end) = self.geom_offsets.start_end(self.geom_index);
        end - start
    }

    fn polygon(&self, i: usize) -> Option<Self::ItemType<'_>> {
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

impl<'a, O: OffsetSizeTrait> MultiPolygonTrait for &'a MultiPolygon<'a, O> {
    type T = f64;
    type ItemType<'b> = Polygon<'a, O> where Self: 'b;
    type Iter<'b> = MultiPolygonIterator<'a, O> where Self: 'b;

    fn polygons(&self) -> Self::Iter<'_> {
        MultiPolygonIterator::new(self)
    }

    fn num_polygons(&self) -> usize {
        let (start, end) = self.geom_offsets.start_end(self.geom_index);
        end - start
    }

    fn polygon(&self, i: usize) -> Option<Self::ItemType<'_>> {
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

impl<O: OffsetSizeTrait> From<MultiPolygon<'_, O>> for geo::MultiPolygon {
    fn from(value: MultiPolygon<'_, O>) -> Self {
        (&value).into()
    }
}

impl<O: OffsetSizeTrait> From<&MultiPolygon<'_, O>> for geo::MultiPolygon {
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

impl<O: OffsetSizeTrait> From<MultiPolygon<'_, O>> for geo::Geometry {
    fn from(value: MultiPolygon<'_, O>) -> Self {
        geo::Geometry::MultiPolygon(value.into())
    }
}

impl<O: OffsetSizeTrait> RTreeObject for MultiPolygon<'_, O> {
    type Envelope = AABB<[f64; 2]>;

    fn envelope(&self) -> Self::Envelope {
        let (lower, upper) = bounding_rect_multipolygon(self);
        AABB::from_corners(lower, upper)
    }
}

impl<O: OffsetSizeTrait> PartialEq for MultiPolygon<'_, O> {
    fn eq(&self, other: &Self) -> bool {
        multi_polygon_eq(self, other)
    }
}

#[cfg(test)]
mod test {
    use crate::array::MultiPolygonArray;
    use crate::test::multipolygon::{mp0, mp1};
    use crate::trait_::GeometryArrayAccessor;

    /// Test Eq where the current index is true but another index is false
    #[test]
    fn test_eq_other_index_false() {
        let arr1: MultiPolygonArray<i32> = vec![mp0(), mp1()].as_slice().into();
        let arr2: MultiPolygonArray<i32> = vec![mp0(), mp0()].as_slice().into();

        assert_eq!(arr1.value(0), arr2.value(0));
        assert_ne!(arr1.value(1), arr2.value(1));
    }
}
