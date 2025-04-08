use crate::algorithm::native::eq::multi_point_eq;
use crate::array::{CoordBuffer, MultiPointArray};
use crate::scalar::{MultiPoint, Point};
use arrow_buffer::OffsetBuffer;
use geo_traits::MultiPointTrait;
use geoarrow_schema::Dimension;

#[derive(Clone, Debug)]
pub struct OwnedMultiPoint {
    coords: CoordBuffer,

    /// Offsets into the coordinate array where each geometry starts
    geom_offsets: OffsetBuffer<i32>,

    geom_index: usize,
}

impl OwnedMultiPoint {
    pub fn new(coords: CoordBuffer, geom_offsets: OffsetBuffer<i32>, geom_index: usize) -> Self {
        Self {
            coords,
            geom_offsets,
            geom_index,
        }
    }
}

impl<'a> From<&'a OwnedMultiPoint> for MultiPoint<'a> {
    fn from(value: &'a OwnedMultiPoint) -> Self {
        Self::new(&value.coords, &value.geom_offsets, value.geom_index)
    }
}

impl From<OwnedMultiPoint> for geo::MultiPoint {
    fn from(value: OwnedMultiPoint) -> Self {
        let geom = MultiPoint::from(&value);
        geom.into()
    }
}

impl<'a> From<MultiPoint<'a>> for OwnedMultiPoint {
    fn from(value: MultiPoint<'a>) -> Self {
        let (coords, geom_offsets, geom_index) = value.into_owned_inner();
        Self::new(coords, geom_offsets, geom_index)
    }
}

impl From<OwnedMultiPoint> for MultiPointArray {
    fn from(value: OwnedMultiPoint) -> Self {
        Self::new(value.coords, value.geom_offsets, None, Default::default())
    }
}

impl MultiPointTrait for OwnedMultiPoint {
    type T = f64;
    type PointType<'b>
        = Point<'b>
    where
        Self: 'b;

    fn dim(&self) -> geo_traits::Dimensions {
        match self.coords.dim() {
            Dimension::XY => geo_traits::Dimensions::Xy,
            Dimension::XYZ => geo_traits::Dimensions::Xyz,
            _ => todo!("XYM and XYZM not supported yet"),
        }
    }

    fn num_points(&self) -> usize {
        MultiPoint::from(self).num_points()
    }

    unsafe fn point_unchecked(&self, i: usize) -> Self::PointType<'_> {
        unsafe { MultiPoint::from(self).point_unchecked(i) }
    }
}

impl<G: MultiPointTrait<T = f64>> PartialEq<G> for OwnedMultiPoint {
    fn eq(&self, other: &G) -> bool {
        multi_point_eq(self, other)
    }
}
