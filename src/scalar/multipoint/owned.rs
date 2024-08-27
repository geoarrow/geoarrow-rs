use crate::algorithm::native::eq::multi_point_eq;
use crate::array::{CoordBuffer, MultiPointArray};
use crate::geo_traits::MultiPointTrait;
use crate::scalar::{MultiPoint, Point};
use arrow_array::OffsetSizeTrait;
use arrow_buffer::OffsetBuffer;

#[derive(Clone, Debug)]
pub struct OwnedMultiPoint<O: OffsetSizeTrait, const D: usize> {
    coords: CoordBuffer<D>,

    /// Offsets into the coordinate array where each geometry starts
    geom_offsets: OffsetBuffer<O>,

    geom_index: usize,
}

impl<O: OffsetSizeTrait, const D: usize> OwnedMultiPoint<O, D> {
    pub fn new(coords: CoordBuffer<D>, geom_offsets: OffsetBuffer<O>, geom_index: usize) -> Self {
        Self {
            coords,
            geom_offsets,
            geom_index,
        }
    }
}

impl<'a, O: OffsetSizeTrait, const D: usize> From<&'a OwnedMultiPoint<O, D>>
    for MultiPoint<'a, O, D>
{
    fn from(value: &'a OwnedMultiPoint<O, D>) -> Self {
        Self::new(&value.coords, &value.geom_offsets, value.geom_index)
    }
}

impl<O: OffsetSizeTrait> From<OwnedMultiPoint<O, 2>> for geo::MultiPoint {
    fn from(value: OwnedMultiPoint<O, 2>) -> Self {
        let geom = MultiPoint::from(&value);
        geom.into()
    }
}

impl<'a, O: OffsetSizeTrait, const D: usize> From<MultiPoint<'a, O, D>> for OwnedMultiPoint<O, D> {
    fn from(value: MultiPoint<'a, O, D>) -> Self {
        let (coords, geom_offsets, geom_index) = value.into_owned_inner();
        Self::new(coords, geom_offsets, geom_index)
    }
}

impl<O: OffsetSizeTrait, const D: usize> From<OwnedMultiPoint<O, D>> for MultiPointArray<O, D> {
    fn from(value: OwnedMultiPoint<O, D>) -> Self {
        Self::new(value.coords, value.geom_offsets, None, Default::default())
    }
}

impl<O: OffsetSizeTrait, const D: usize> MultiPointTrait for OwnedMultiPoint<O, D> {
    type T = f64;
    type ItemType<'b> = Point<'b, D> where Self: 'b;

    fn dim(&self) -> usize {
        D
    }

    fn num_points(&self) -> usize {
        MultiPoint::from(self).num_points()
    }

    unsafe fn point_unchecked(&self, i: usize) -> Self::ItemType<'_> {
        MultiPoint::from(self).point_unchecked(i)
    }
}

impl<O: OffsetSizeTrait, G: MultiPointTrait<T = f64>> PartialEq<G> for OwnedMultiPoint<O, 2> {
    fn eq(&self, other: &G) -> bool {
        multi_point_eq(self, other)
    }
}
