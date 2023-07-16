use crate::algorithm::native::bounding_rect::bounding_rect_point;
use crate::array::CoordBuffer;
use crate::geo_traits::{CoordTrait, PointTrait};
use rstar::{RTreeObject, AABB};

/// An Arrow equivalent of a Point
#[derive(Debug, Clone, PartialEq)]
pub struct Point<'a> {
    pub coords: &'a CoordBuffer,
    pub geom_index: usize,
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
