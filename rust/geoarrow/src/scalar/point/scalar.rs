use crate::algorithm::native::eq::point_eq;
use crate::array::CoordBuffer;
use crate::scalar::Coord;
use crate::trait_::NativeScalar;
use geo_traits::to_geo::ToGeoPoint;
use geo_traits::{CoordTrait, PointTrait};
use rstar::{AABB, RTreeObject};

/// An Arrow equivalent of a Point
///
/// This implements [PointTrait], which you can use to extract data.
#[derive(Debug, Clone)]
pub struct Point<'a> {
    coords: &'a CoordBuffer,
    geom_index: usize,
}

impl<'a> Point<'a> {
    pub(crate) fn new(coords: &'a CoordBuffer, geom_index: usize) -> Self {
        Point { coords, geom_index }
    }

    pub(crate) fn into_owned_inner(self) -> (CoordBuffer, usize) {
        (self.coords.clone(), self.geom_index)
    }
}

impl NativeScalar for Point<'_> {
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

impl<'a> PointTrait for Point<'a> {
    type T = f64;
    type CoordType<'b>
        = Coord<'a>
    where
        Self: 'b;

    fn dim(&self) -> geo_traits::Dimensions {
        self.coords.dim().into()
    }

    fn coord(&self) -> Option<Self::CoordType<'_>> {
        let coord = self.coords.value(self.geom_index);
        if coord.is_nan() { None } else { Some(coord) }
    }
}

impl<'a> PointTrait for &Point<'a> {
    type T = f64;
    type CoordType<'b>
        = Coord<'a>
    where
        Self: 'b;

    fn dim(&self) -> geo_traits::Dimensions {
        self.coords.dim().into()
    }

    fn coord(&self) -> Option<Self::CoordType<'_>> {
        let coord = self.coords.value(self.geom_index);
        if coord.is_nan() { None } else { Some(coord) }
    }
}

impl From<Point<'_>> for geo::Point {
    fn from(value: Point<'_>) -> Self {
        (&value).into()
    }
}

impl From<&Point<'_>> for geo::Point {
    fn from(value: &Point<'_>) -> Self {
        value.to_point()
    }
}

impl From<Point<'_>> for geo::Geometry {
    fn from(value: Point<'_>) -> Self {
        geo::Geometry::Point(value.into())
    }
}

impl RTreeObject for Point<'_> {
    type Envelope = AABB<[f64; 2]>;

    fn envelope(&self) -> Self::Envelope {
        AABB::from_point([self.coord().unwrap().x(), self.coord().unwrap().y()])
    }
}

impl<G: PointTrait<T = f64>> PartialEq<G> for Point<'_> {
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
