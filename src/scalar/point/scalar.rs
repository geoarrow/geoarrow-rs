use crate::algorithm::native::bounding_rect::bounding_rect_point;
use crate::algorithm::native::eq::point_eq;
use crate::array::CoordBuffer;
use crate::geo_traits::{CoordTrait, PointTrait};
use crate::trait_::GeometryScalarTrait;
use rstar::{RTreeObject, AABB};
use std::borrow::Cow;

/// An Arrow equivalent of a Point
#[derive(Debug)]
pub struct Point<'a> {
    coords: Cow<'a, CoordBuffer>,
    geom_index: usize,
}

impl<'a> ToOwned for Point<'a> {
    type Owned = Point<'a>;

    fn to_owned(&self) -> Self::Owned {
        let (cb, geom_index) = match &self.coords {
            Cow::Owned(cb) => (cb, self.geom_index),
            // TODO: create new arrays that aren't linked to the existing array
            // TODO: this geom_index will become 0
            Cow::Borrowed(cb) => (cb.to_owned(), self.geom_index),
        };

        Point::new_owned(cb.clone(), geom_index)
    }
}

impl<'a> Point<'a> {
    pub fn new(coords: Cow<'a, CoordBuffer>, geom_index: usize) -> Self {
        Point { coords, geom_index }
    }

    pub fn new_borrowed(coords: &'a CoordBuffer, geom_index: usize) -> Self {
        Point {
            coords: Cow::Borrowed(coords),
            geom_index,
        }
    }

    pub fn new_owned(coords: CoordBuffer, geom_index: usize) -> Self {
        Point {
            coords: Cow::Owned(coords),
            geom_index,
        }
    }
}

impl<'a> GeometryScalarTrait<'a> for Point<'a> {
    type ScalarGeo = geo::Point;

    fn to_geo(&self) -> Self::ScalarGeo {
        self.into()
    }
}

impl PointTrait for Point<'_> {
    type T = f64;

    fn x(&self) -> f64 {
        self.coords.get_x(self.geom_index)
    }

    fn y(&self) -> f64 {
        self.coords.get_y(self.geom_index)
    }
}

impl PointTrait for &Point<'_> {
    type T = f64;

    fn x(&self) -> f64 {
        self.coords.get_x(self.geom_index)
    }

    fn y(&self) -> f64 {
        self.coords.get_y(self.geom_index)
    }
}

impl CoordTrait for Point<'_> {
    type T = f64;

    fn x(&self) -> Self::T {
        self.coords.get_x(self.geom_index)
    }

    fn y(&self) -> Self::T {
        self.coords.get_y(self.geom_index)
    }
}

impl From<Point<'_>> for geo::Point {
    fn from(value: Point<'_>) -> Self {
        (&value).into()
    }
}

impl From<&Point<'_>> for geo::Point {
    fn from(value: &Point<'_>) -> Self {
        geo::Point::new(PointTrait::x(&value), PointTrait::y(&value))
    }
}

impl From<Point<'_>> for geo::Coord {
    fn from(value: Point<'_>) -> Self {
        (&value).into()
    }
}

impl From<&Point<'_>> for geo::Coord {
    fn from(value: &Point<'_>) -> Self {
        geo::Coord {
            x: PointTrait::x(&value),
            y: PointTrait::y(&value),
        }
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
        let (lower, upper) = bounding_rect_point(self);
        AABB::from_corners(lower, upper)
    }
}

impl PartialEq for Point<'_> {
    fn eq(&self, other: &Self) -> bool {
        point_eq(self, other)
    }
}

#[cfg(test)]
mod test {
    use crate::array::{CoordBuffer, PointArray};
    use crate::GeometryArrayTrait;

    /// Test Eq where the current index is true but another index is false
    #[test]
    fn test_eq_other_index_false() {
        let x1 = vec![0., 1., 2.];
        let y1 = vec![3., 4., 5.];
        let buf1 = CoordBuffer::Separated((x1, y1).try_into().unwrap());
        let arr1 = PointArray::new(buf1, None);

        let x2 = vec![0., 100., 2.];
        let y2 = vec![3., 400., 5.];
        let buf2 = CoordBuffer::Separated((x2, y2).try_into().unwrap());
        let arr2 = PointArray::new(buf2, None);

        assert_eq!(arr1.value(0), arr2.value(0));
    }
}
