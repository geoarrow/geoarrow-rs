use crate::algorithm::native::bounding_rect::bounding_rect_point;
use crate::algorithm::native::eq::point_eq;
use crate::array::CoordBuffer;
use crate::geo_traits::{CoordTrait, PointTrait};
use crate::io::geo::{coord_to_geo, point_to_geo};
use crate::scalar::Coord;
use crate::trait_::{GeometryArrayAccessor, GeometryArraySelfMethods, GeometryScalarTrait};
use rstar::{RTreeObject, AABB};
use std::borrow::Cow;

/// An Arrow equivalent of a Point
#[derive(Debug, Clone)]
pub struct Point<'a, const D: usize> {
    coords: Cow<'a, CoordBuffer<D>>,
    geom_index: usize,
}

// TODO: should we have this or clone?
// impl<'a> ToOwned for Point<'a> {
//     type Owned = Point<'a>;

//     fn to_owned(&self) -> Self::Owned {
//         match &self.coords {
//             Cow::Owned(cb) => Point::new_owned(cb.clone(), self.geom_index),
//             Cow::Borrowed(cb) => {
//                 // TODO: DRY this with array impl
//                 let coords = cb.owned_slice(self.geom_index, 1);
//                 Self::new_owned(coords, 0)
//             },
//         }
//     }
// }

impl<'a, const D: usize> Point<'a, D> {
    pub fn new(coords: Cow<'a, CoordBuffer<D>>, geom_index: usize) -> Self {
        Point { coords, geom_index }
    }

    pub fn new_borrowed(coords: &'a CoordBuffer<D>, geom_index: usize) -> Self {
        Point {
            coords: Cow::Borrowed(coords),
            geom_index,
        }
    }

    pub fn new_owned(coords: CoordBuffer<D>, geom_index: usize) -> Self {
        Point {
            coords: Cow::Owned(coords),
            geom_index,
        }
    }

    pub fn coord(&self) -> Coord<D> {
        self.coords.value(self.geom_index)
    }

    /// Extracts the owned data.
    ///
    /// Clones the data if it is not already owned.
    pub fn into_owned(self) -> Self {
        match self.coords {
            Cow::Owned(cb) => Self::new_owned(cb, self.geom_index),
            Cow::Borrowed(cb) => {
                // TODO: should this just take the overhead of converting to a point array and slicing that?
                // TODO: DRY this with array impl
                let coords = cb.owned_slice(self.geom_index, 1);
                Self::new_owned(coords, 0)
            }
        }
    }

    pub fn into_owned_inner(self) -> (CoordBuffer<D>, usize) {
        let owned = self.into_owned();
        (owned.coords.into_owned(), owned.geom_index)
    }
}

impl<'a, const D: usize> GeometryScalarTrait for Point<'a, D> {
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

impl<const D: usize> PointTrait for Point<'_, D> {
    type T = f64;

    fn dim(&self) -> usize {
        D
    }

    fn nth_unchecked(&self, n: usize) -> Self::T {
        let coord = self.coords.value(self.geom_index);
        CoordTrait::nth_unchecked(&coord, n)
    }

    fn x(&self) -> f64 {
        self.coords.get_x(self.geom_index)
    }

    fn y(&self) -> f64 {
        self.coords.get_y(self.geom_index)
    }
}

impl<const D: usize> PointTrait for &Point<'_, D> {
    type T = f64;

    fn dim(&self) -> usize {
        D
    }

    fn nth_unchecked(&self, n: usize) -> Self::T {
        let coord = self.coords.value(self.geom_index);
        CoordTrait::nth_unchecked(&coord, n)
    }

    fn x(&self) -> f64 {
        self.coords.get_x(self.geom_index)
    }

    fn y(&self) -> f64 {
        self.coords.get_y(self.geom_index)
    }
}

impl<const D: usize> CoordTrait for Point<'_, D> {
    type T = f64;

    fn dim(&self) -> usize {
        D
    }

    fn nth_unchecked(&self, n: usize) -> Self::T {
        let coord = self.coords.value(self.geom_index);
        CoordTrait::nth_unchecked(&coord, n)
    }

    fn x(&self) -> Self::T {
        self.coords.get_x(self.geom_index)
    }

    fn y(&self) -> Self::T {
        self.coords.get_y(self.geom_index)
    }
}

impl<const D: usize> From<Point<'_, D>> for geo::Point {
    fn from(value: Point<'_, D>) -> Self {
        (&value).into()
    }
}

impl<const D: usize> From<&Point<'_, D>> for geo::Point {
    fn from(value: &Point<'_, D>) -> Self {
        point_to_geo(value)
    }
}

impl<const D: usize> From<Point<'_, D>> for geo::Coord {
    fn from(value: Point<'_, D>) -> Self {
        (&value).into()
    }
}

impl<const D: usize> From<&Point<'_, D>> for geo::Coord {
    fn from(value: &Point<'_, D>) -> Self {
        coord_to_geo(value)
    }
}

impl<const D: usize> From<Point<'_, D>> for geo::Geometry {
    fn from(value: Point<'_, D>) -> Self {
        geo::Geometry::Point(value.into())
    }
}

impl<const D: usize> RTreeObject for Point<'_, D> {
    type Envelope = AABB<[f64; 2]>;

    fn envelope(&self) -> Self::Envelope {
        let (lower, upper) = bounding_rect_point(self);
        AABB::from_corners(lower, upper)
    }
}

impl<G: PointTrait<T = f64>, const D: usize> PartialEq<G> for Point<'_, D> {
    fn eq(&self, other: &G) -> bool {
        point_eq(self, other, true)
    }
}

#[cfg(test)]
mod test {
    use crate::array::{CoordBuffer, PointArray};
    use crate::trait_::GeometryArrayAccessor;

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
