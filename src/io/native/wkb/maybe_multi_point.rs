use crate::geo_traits::MultiPointTrait;
use crate::io::native::wkb::multipoint::WKBMultiPoint;
use crate::io::native::wkb::point::WKBPoint;
// use crate::io::native::wkb::Point::WKBPoint;
// use crate::io::native::wkb::multiPoint::WKBMultiPoint;
use std::iter::Cloned;
use std::slice::Iter;

/// An WKB object that can be either a WKBPoint or a WKBMultiPoint.
///
/// This is used for casting a mix of Points and multi Points to an array of multi Points
#[derive(Debug, Clone, Copy)]
pub enum WKBMaybeMultiPoint<'a> {
    Point(WKBPoint<'a>),
    MultiPoint(WKBMultiPoint<'a>),
}

impl<'a> MultiPointTrait<'a> for WKBMaybeMultiPoint<'a> {
    type T = f64;
    type ItemType = WKBPoint<'a>;
    type Iter = Cloned<Iter<'a, Self::ItemType>>;

    fn num_points(&self) -> usize {
        match self {
            WKBMaybeMultiPoint::Point(geom) => geom.num_points(),
            WKBMaybeMultiPoint::MultiPoint(geom) => geom.num_points(),
        }
    }

    fn point(&self, i: usize) -> Option<Self::ItemType> {
        match self {
            WKBMaybeMultiPoint::Point(geom) => geom.point(i),
            WKBMaybeMultiPoint::MultiPoint(geom) => geom.point(i),
        }
    }

    fn points(&'a self) -> Self::Iter {
        match self {
            WKBMaybeMultiPoint::Point(geom) => geom.points(),
            WKBMaybeMultiPoint::MultiPoint(geom) => geom.points(),
        }
    }
}

impl<'a> MultiPointTrait<'a> for &WKBMaybeMultiPoint<'a> {
    type T = f64;
    type ItemType = WKBPoint<'a>;
    type Iter = Cloned<Iter<'a, Self::ItemType>>;

    fn num_points(&self) -> usize {
        match self {
            WKBMaybeMultiPoint::Point(geom) => geom.num_points(),
            WKBMaybeMultiPoint::MultiPoint(geom) => geom.num_points(),
        }
    }

    fn point(&self, i: usize) -> Option<Self::ItemType> {
        match self {
            WKBMaybeMultiPoint::Point(geom) => geom.point(i),
            WKBMaybeMultiPoint::MultiPoint(geom) => geom.point(i),
        }
    }

    fn points(&'a self) -> Self::Iter {
        match self {
            WKBMaybeMultiPoint::Point(geom) => geom.points(),
            WKBMaybeMultiPoint::MultiPoint(geom) => geom.points(),
        }
    }
}
