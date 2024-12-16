use crate::algorithm::native::bounding_rect::bounding_rect_point;
use crate::algorithm::native::eq::point_eq;
use crate::array::PointArray;
use crate::scalar::Coord;
use crate::trait_::NativeScalar;
use crate::{ArrayBase, NativeArray};
use geo_traits::to_geo::ToGeoPoint;
use geo_traits::PointTrait;
use rstar::{RTreeObject, AABB};

/// An Arrow equivalent of a Point
///
/// This is stored as a [PointArray] with length 1. That element may not be null.
#[derive(Debug, Clone)]
pub struct Point(PointArray);

impl Point {
    pub fn new(arr: PointArray) -> Self {
        assert_eq!(arr.len(), 1);
        assert!(!arr.is_null(0));
        Self(arr)
    }

    pub fn into_inner(self) -> PointArray {
        self.0
    }
}

impl NativeScalar for Point {
    type ScalarGeo = geo::Point;

    fn to_geo(&self) -> Self::ScalarGeo {
        self.into()
    }

    fn to_geo_geometry(&self) -> geo::Geometry {
        geo::Geometry::Point(self.to_geo())
    }

    #[cfg(feature = "geos")]
    fn to_geos(&self) -> std::result::Result<geos::Geometry, geos::Error> {
        self.try_into()
    }
}

impl PointTrait for Point {
    type T = f64;
    type CoordType<'b>
        = Coord
    where
        Self: 'b;

    fn dim(&self) -> geo_traits::Dimensions {
        self.0.dimension().into()
    }

    fn coord(&self) -> Option<Self::CoordType<'_>> {
        let coord = self.0.coords.value(0);
        if coord.is_nan() {
            None
        } else {
            Some(coord)
        }
    }
}

impl PointTrait for &Point {
    type T = f64;
    type CoordType<'b>
        = Coord
    where
        Self: 'b;

    fn dim(&self) -> geo_traits::Dimensions {
        self.0.dimension().into()
    }

    fn coord(&self) -> Option<Self::CoordType<'_>> {
        let coord = self.0.coords.value(0);
        if coord.is_nan() {
            None
        } else {
            Some(coord)
        }
    }
}

impl From<Point> for geo::Point {
    fn from(value: Point) -> Self {
        (&value).into()
    }
}

impl From<&Point> for geo::Point {
    fn from(value: &Point) -> Self {
        value.to_point()
    }
}

impl From<Point> for geo::Geometry {
    fn from(value: Point) -> Self {
        geo::Geometry::Point(value.into())
    }
}

impl RTreeObject for Point {
    type Envelope = AABB<[f64; 2]>;

    fn envelope(&self) -> Self::Envelope {
        let (lower, upper) = bounding_rect_point(self);
        AABB::from_corners(lower, upper)
    }
}

impl<G: PointTrait<T = f64>> PartialEq<G> for Point {
    fn eq(&self, other: &G) -> bool {
        point_eq(self, other)
    }
}

#[cfg(test)]
mod test {
    use crate::array::{CoordBuffer, PointArray};
    use crate::trait_::ArrayAccessor;

    /// Test Eq where the current index is true but another index is false
    #[test]
    fn test_eq_other_index_false() {
        let x1 = vec![0., 1., 2.];
        let y1 = vec![3., 4., 5.];
        let buf1 = CoordBuffer::Separated((x1, y1).try_into().unwrap());
        let arr1 = PointArray::new(buf1, None, Default::default());

        let x2 = vec![0., 100., 2.];
        let y2 = vec![3., 400., 5.];
        let buf2 = CoordBuffer::Separated((x2, y2).try_into().unwrap());
        let arr2 = PointArray::new(buf2, None, Default::default());

        assert_eq!(arr1.value(0), arr2.value(0));
    }
}
