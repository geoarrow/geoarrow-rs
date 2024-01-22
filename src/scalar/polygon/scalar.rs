use crate::algorithm::native::bounding_rect::bounding_rect_polygon;
use crate::algorithm::native::eq::polygon_eq;
use crate::array::util::OffsetBufferUtils;
use crate::array::{CoordBuffer, PolygonArray};
use crate::geo_traits::PolygonTrait;
use crate::io::geo::polygon_to_geo;
use crate::scalar::LineString;
use crate::trait_::{GeometryArraySelfMethods, GeometryScalarTrait};
use arrow_array::OffsetSizeTrait;
use arrow_buffer::OffsetBuffer;
use rstar::{RTreeObject, AABB};
use std::borrow::Cow;

/// An Arrow equivalent of a Polygon
#[derive(Debug, Clone)]
pub struct Polygon<'a, O: OffsetSizeTrait> {
    pub(crate) coords: Cow<'a, CoordBuffer>,

    /// Offsets into the ring array where each geometry starts
    pub(crate) geom_offsets: Cow<'a, OffsetBuffer<O>>,

    /// Offsets into the coordinate array where each ring starts
    pub(crate) ring_offsets: Cow<'a, OffsetBuffer<O>>,

    pub(crate) geom_index: usize,

    start_offset: usize,
}

impl<'a, O: OffsetSizeTrait> Polygon<'a, O> {
    pub fn new(
        coords: Cow<'a, CoordBuffer>,
        geom_offsets: Cow<'a, OffsetBuffer<O>>,
        ring_offsets: Cow<'a, OffsetBuffer<O>>,
        geom_index: usize,
    ) -> Self {
        let (start_offset, _) = geom_offsets.start_end(geom_index);
        Self {
            coords,
            geom_offsets,
            ring_offsets,
            geom_index,
            start_offset,
        }
    }

    pub fn new_borrowed(
        coords: &'a CoordBuffer,
        geom_offsets: &'a OffsetBuffer<O>,
        ring_offsets: &'a OffsetBuffer<O>,
        geom_index: usize,
    ) -> Self {
        Self::new(
            Cow::Borrowed(coords),
            Cow::Borrowed(geom_offsets),
            Cow::Borrowed(ring_offsets),
            geom_index,
        )
    }

    pub fn new_owned(
        coords: CoordBuffer,
        geom_offsets: OffsetBuffer<O>,
        ring_offsets: OffsetBuffer<O>,
        geom_index: usize,
    ) -> Self {
        Self::new(
            Cow::Owned(coords),
            Cow::Owned(geom_offsets),
            Cow::Owned(ring_offsets),
            geom_index,
        )
    }

    /// Extracts the owned data.
    ///
    /// Clones the data if it is not already owned.
    pub fn into_owned(self) -> Self {
        let arr = PolygonArray::new(
            self.coords.into_owned(),
            self.geom_offsets.into_owned(),
            self.ring_offsets.into_owned(),
            None,
            Default::default(),
        );
        let sliced_arr = arr.owned_slice(self.geom_index, 1);
        Self::new_owned(
            sliced_arr.coords,
            sliced_arr.geom_offsets,
            sliced_arr.ring_offsets,
            0,
        )
    }

    pub fn into_owned_inner(self) -> (CoordBuffer, OffsetBuffer<O>, OffsetBuffer<O>, usize) {
        let owned = self.into_owned();
        (
            owned.coords.into_owned(),
            owned.geom_offsets.into_owned(),
            owned.ring_offsets.into_owned(),
            owned.geom_index,
        )
    }
}

impl<'a, O: OffsetSizeTrait> GeometryScalarTrait for Polygon<'a, O> {
    type ScalarGeo = geo::Polygon;

    fn to_geo(&self) -> Self::ScalarGeo {
        self.into()
    }

    #[cfg(feature = "geos")]
    fn to_geos(&self) -> std::result::Result<geos::Geometry, geos::Error> {
        self.try_into()
    }
}

impl<'a, O: OffsetSizeTrait> PolygonTrait for Polygon<'a, O> {
    type T = f64;
    type ItemType<'b> = LineString<'a, O> where Self: 'b;

    fn exterior(&self) -> Option<Self::ItemType<'_>> {
        let (start, end) = self.geom_offsets.start_end(self.geom_index);
        if start == end {
            None
        } else {
            Some(LineString::new(
                self.coords.clone(),
                self.ring_offsets.clone(),
                start,
            ))
        }
    }

    fn num_interiors(&self) -> usize {
        let (start, end) = self.geom_offsets.start_end(self.geom_index);
        end - start - 1
    }

    unsafe fn interior_unchecked(&self, i: usize) -> Self::ItemType<'_> {
        LineString::new(
            self.coords.clone(),
            self.ring_offsets.clone(),
            self.start_offset + 1 + i,
        )
    }
}

impl<'a, O: OffsetSizeTrait> PolygonTrait for &'a Polygon<'a, O> {
    type T = f64;
    type ItemType<'b> = LineString<'a, O> where Self: 'b;

    fn exterior(&self) -> Option<Self::ItemType<'_>> {
        let (start, end) = self.geom_offsets.start_end(self.geom_index);
        if start == end {
            None
        } else {
            Some(LineString::new(
                self.coords.clone(),
                self.ring_offsets.clone(),
                start,
            ))
        }
    }

    fn num_interiors(&self) -> usize {
        let (start, end) = self.geom_offsets.start_end(self.geom_index);
        end - start - 1
    }

    unsafe fn interior_unchecked(&self, i: usize) -> Self::ItemType<'_> {
        LineString::new(
            self.coords.clone(),
            self.ring_offsets.clone(),
            self.start_offset + 1 + i,
        )
    }
}

impl<O: OffsetSizeTrait> From<Polygon<'_, O>> for geo::Polygon {
    fn from(value: Polygon<'_, O>) -> Self {
        (&value).into()
    }
}

impl<O: OffsetSizeTrait> From<&Polygon<'_, O>> for geo::Polygon {
    fn from(value: &Polygon<'_, O>) -> Self {
        polygon_to_geo(value)
    }
}

impl<O: OffsetSizeTrait> From<Polygon<'_, O>> for geo::Geometry {
    fn from(value: Polygon<'_, O>) -> Self {
        geo::Geometry::Polygon(value.into())
    }
}

impl<O: OffsetSizeTrait> RTreeObject for Polygon<'_, O> {
    type Envelope = AABB<[f64; 2]>;

    fn envelope(&self) -> Self::Envelope {
        let (lower, upper) = bounding_rect_polygon(self);
        AABB::from_corners(lower, upper)
    }
}

impl<O: OffsetSizeTrait, G: PolygonTrait<T = f64>> PartialEq<G> for Polygon<'_, O> {
    fn eq(&self, other: &G) -> bool {
        polygon_eq(self, other)
    }
}

#[cfg(test)]
mod test {
    use crate::array::PolygonArray;
    use crate::test::polygon::{p0, p1};
    use crate::trait_::GeometryArrayAccessor;

    /// Test Eq where the current index is true but another index is false
    #[test]
    fn test_eq_other_index_false() {
        let arr1: PolygonArray<i32> = vec![p0(), p1()].as_slice().into();
        let arr2: PolygonArray<i32> = vec![p0(), p0()].as_slice().into();

        assert_eq!(arr1.value(0), arr2.value(0));
        assert_ne!(arr1.value(1), arr2.value(1));
    }
}
