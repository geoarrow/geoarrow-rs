use crate::algorithm::native::eq::multi_point_eq;
use crate::geo_traits::MultiPointTrait;
use crate::io::wkb::reader::multipoint::WKBMultiPoint;
use crate::io::wkb::reader::point::WKBPoint;
// use crate::io::wkb::Point::WKBPoint;
// use crate::io::wkb::multiPoint::WKBMultiPoint;
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

impl<'a> WKBMaybeMultiPoint<'a> {
    /// Check if this has equal coordinates as some other MultiPoint object
    pub fn equals_multi_point(&self, other: &impl MultiPointTrait<T = f64>) -> bool {
        multi_point_eq(self, other)
    }
}

impl<'a> MultiPointTrait for WKBMaybeMultiPoint<'a> {
    type T = f64;
    type ItemType<'b> = WKBPoint<'a> where Self: 'b;
    type Iter<'b> = Cloned<Iter<'a, Self::ItemType<'a>>> where Self: 'b;

    fn num_points(&self) -> usize {
        match self {
            WKBMaybeMultiPoint::Point(geom) => geom.num_points(),
            WKBMaybeMultiPoint::MultiPoint(geom) => geom.num_points(),
        }
    }

    fn point(&self, i: usize) -> Option<Self::ItemType<'_>> {
        match self {
            WKBMaybeMultiPoint::Point(geom) => geom.point(i),
            WKBMaybeMultiPoint::MultiPoint(geom) => geom.point(i),
        }
    }

    fn points(&self) -> Self::Iter<'_> {
        match self {
            WKBMaybeMultiPoint::Point(geom) => geom.points(),
            WKBMaybeMultiPoint::MultiPoint(geom) => geom.points(),
        }
    }
}

impl<'a> MultiPointTrait for &'a WKBMaybeMultiPoint<'a> {
    type T = f64;
    type ItemType<'b> = WKBPoint<'a> where Self: 'b;
    type Iter<'b> = Cloned<Iter<'a, Self::ItemType<'a>>> where Self: 'b;

    fn num_points(&self) -> usize {
        match self {
            WKBMaybeMultiPoint::Point(geom) => geom.num_points(),
            WKBMaybeMultiPoint::MultiPoint(geom) => geom.num_points(),
        }
    }

    fn point(&self, i: usize) -> Option<Self::ItemType<'_>> {
        match self {
            WKBMaybeMultiPoint::Point(geom) => geom.point(i),
            WKBMaybeMultiPoint::MultiPoint(geom) => geom.point(i),
        }
    }

    fn points(&self) -> Self::Iter<'_> {
        match self {
            WKBMaybeMultiPoint::Point(geom) => geom.points(),
            WKBMaybeMultiPoint::MultiPoint(geom) => geom.points(),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::io::wkb::reader::geometry::Endianness;
    use crate::test::multipoint::mp0;
    use crate::test::point::p0;
    use geozero::{CoordDimensions, ToWkb};

    #[test]
    fn point_round_trip() {
        let geom = p0();
        let buf = geo::Geometry::Point(geom)
            .to_wkb(CoordDimensions::xy())
            .unwrap();
        let wkb_geom = WKBMaybeMultiPoint::Point(WKBPoint::new(&buf, Endianness::LittleEndian, 0));

        assert!(wkb_geom.equals_multi_point(&geo::MultiPoint(vec![geom])));
    }

    #[test]
    fn multi_point_round_trip() {
        let geom = mp0();
        let buf = geo::Geometry::MultiPoint(geom.clone())
            .to_wkb(CoordDimensions::xy())
            .unwrap();
        let wkb_geom =
            WKBMaybeMultiPoint::MultiPoint(WKBMultiPoint::new(&buf, Endianness::LittleEndian));

        assert!(wkb_geom.equals_multi_point(&geom));
    }
}
