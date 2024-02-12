use crate::algorithm::native::eq::multi_point_eq;
use crate::array::CoordBuffer;
use crate::geo_traits::MultiPointTrait;
use crate::scalar::{MultiPoint, Point};
use arrow_array::OffsetSizeTrait;
use arrow_buffer::OffsetBuffer;

#[derive(Debug)]
pub struct OwnedMultiPoint<O: OffsetSizeTrait> {
    coords: CoordBuffer,

    /// Offsets into the coordinate array where each geometry starts
    geom_offsets: OffsetBuffer<O>,

    geom_index: usize,
}

impl<O: OffsetSizeTrait> OwnedMultiPoint<O> {
    pub fn new(coords: CoordBuffer, geom_offsets: OffsetBuffer<O>, geom_index: usize) -> Self {
        Self {
            coords,
            geom_offsets,
            geom_index,
        }
    }
}

impl<'a, O: OffsetSizeTrait> From<OwnedMultiPoint<O>> for MultiPoint<'a, O> {
    fn from(value: OwnedMultiPoint<O>) -> Self {
        Self::new_owned(value.coords, value.geom_offsets, value.geom_index)
    }
}

impl<'a, O: OffsetSizeTrait> From<&'a OwnedMultiPoint<O>> for MultiPoint<'a, O> {
    fn from(value: &'a OwnedMultiPoint<O>) -> Self {
        Self::new_borrowed(&value.coords, &value.geom_offsets, value.geom_index)
    }
}

impl<O: OffsetSizeTrait> From<OwnedMultiPoint<O>> for geo::MultiPoint {
    fn from(value: OwnedMultiPoint<O>) -> Self {
        let geom = MultiPoint::from(value);
        geom.into()
    }
}

impl<'a, O: OffsetSizeTrait> From<MultiPoint<'a, O>> for OwnedMultiPoint<O> {
    fn from(value: MultiPoint<'a, O>) -> Self {
        let (coords, geom_offsets, geom_index) = value.into_owned_inner();
        Self::new(coords, geom_offsets, geom_index)
    }
}

impl<O: OffsetSizeTrait> MultiPointTrait for OwnedMultiPoint<O> {
    type T = f64;
    type ItemType<'b> = Point<'b> where Self: 'b;

    fn num_points(&self) -> usize {
        MultiPoint::from(self).num_points()
    }

    unsafe fn point_unchecked(&self, i: usize) -> Self::ItemType<'_> {
        MultiPoint::from(self).point_unchecked(i)
    }
}

impl<O: OffsetSizeTrait, G: MultiPointTrait<T = f64>> PartialEq<G> for OwnedMultiPoint<O> {
    fn eq(&self, other: &G) -> bool {
        multi_point_eq(self, other)
    }
}
