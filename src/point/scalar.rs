use crate::algorithm::bounding_rect::bounding_rect_point;
use crate::geo_traits::PointTrait;
use crate::CoordBuffer;
use rstar::{RTreeObject, AABB};

/// An Arrow equivalent of a Point
#[derive(Debug, Clone)]
pub struct Point<'a> {
    pub coords: &'a CoordBuffer,
    pub geom_index: usize,
}

impl PointTrait for Point<'_> {
    fn x(&self) -> f64 {
        self.coords.get_x(self.geom_index)
    }

    fn y(&self) -> f64 {
        self.coords.get_y(self.geom_index)
    }

    fn x_y(&self) -> (f64, f64) {
        (self.x(), self.y())
    }
}

impl PointTrait for &Point<'_> {
    fn x(&self) -> f64 {
        self.coords.get_x(self.geom_index)
    }

    fn y(&self) -> f64 {
        self.coords.get_y(self.geom_index)
    }

    fn x_y(&self) -> (f64, f64) {
        (self.x(), self.y())
    }
}

impl From<Point<'_>> for geo::Point {
    fn from(value: Point<'_>) -> Self {
        (&value).into()
    }
}

impl From<&Point<'_>> for geo::Point {
    fn from(value: &Point<'_>) -> Self {
        geo::Point::new(value.x(), value.y())
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
            x: value.x(),
            y: value.y(),
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
