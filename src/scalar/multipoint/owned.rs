use crate::algorithm::native::eq::multi_point_eq;
use crate::array::{CoordBuffer, MultiPointArray};
use crate::geo_traits::MultiPointTrait;
use crate::scalar::{MultiPoint, Point};
use arrow_buffer::OffsetBuffer;

#[derive(Clone, Debug)]
pub struct OwnedMultiPoint<const D: usize> {
    coords: CoordBuffer<D>,

    /// Offsets into the coordinate array where each geometry starts
    geom_offsets: OffsetBuffer<i32>,

    geom_index: usize,
}

impl<const D: usize> OwnedMultiPoint<D> {
    pub fn new(coords: CoordBuffer<D>, geom_offsets: OffsetBuffer<i32>, geom_index: usize) -> Self {
        Self {
            coords,
            geom_offsets,
            geom_index,
        }
    }
}

impl<'a, const D: usize> From<&'a OwnedMultiPoint<D>> for MultiPoint<'a, D> {
    fn from(value: &'a OwnedMultiPoint<D>) -> Self {
        Self::new(&value.coords, &value.geom_offsets, value.geom_index)
    }
}

impl From<OwnedMultiPoint<2>> for geo::MultiPoint {
    fn from(value: OwnedMultiPoint<2>) -> Self {
        let geom = MultiPoint::from(&value);
        geom.into()
    }
}

impl<'a, const D: usize> From<MultiPoint<'a, D>> for OwnedMultiPoint<D> {
    fn from(value: MultiPoint<'a, D>) -> Self {
        let (coords, geom_offsets, geom_index) = value.into_owned_inner();
        Self::new(coords, geom_offsets, geom_index)
    }
}

impl<const D: usize> From<OwnedMultiPoint<D>> for MultiPointArray<D> {
    fn from(value: OwnedMultiPoint<D>) -> Self {
        Self::new(value.coords, value.geom_offsets, None, Default::default())
    }
}

impl<const D: usize> MultiPointTrait for OwnedMultiPoint<D> {
    type T = f64;
    type ItemType<'b> = Point<'b, D> where Self: 'b;

    fn dim(&self) -> crate::geo_traits::Dimension {
        // TODO: pass through field information from array
        match D {
            2 => crate::geo_traits::Dimension::XY,
            3 => crate::geo_traits::Dimension::XYZ,
            _ => todo!(),
        }
    }

    fn num_points(&self) -> usize {
        MultiPoint::from(self).num_points()
    }

    unsafe fn point_unchecked(&self, i: usize) -> Self::ItemType<'_> {
        MultiPoint::from(self).point_unchecked(i)
    }
}

impl<G: MultiPointTrait<T = f64>> PartialEq<G> for OwnedMultiPoint<2> {
    fn eq(&self, other: &G) -> bool {
        multi_point_eq(self, other)
    }
}
