use arrow_buffer::ScalarBuffer;
use rstar::{RTreeObject, AABB};
use std::borrow::Cow;

use crate::geo_traits::RectTrait;
use crate::trait_::GeometryScalarTrait;

#[derive(Debug, Clone)]
pub struct Rect<'a> {
    pub values: Cow<'a, ScalarBuffer<f64>>,
    pub geom_index: usize,
}

impl<'a> Rect<'a> {
    pub fn new(values: Cow<'a, ScalarBuffer<f64>>, geom_index: usize) -> Self {
        Self { values, geom_index }
    }

    pub fn new_borrowed(values: &'a ScalarBuffer<f64>, geom_index: usize) -> Self {
        Self {
            values: Cow::Borrowed(values),
            geom_index,
        }
    }

    pub fn new_owned(values: ScalarBuffer<f64>, geom_index: usize) -> Self {
        Self {
            values: Cow::Owned(values),
            geom_index,
        }
    }
}

impl<'a> GeometryScalarTrait for Rect<'a> {
    type ScalarGeo = geo::Rect;

    fn to_geo(&self) -> Self::ScalarGeo {
        self.into()
    }
}

impl<'a> RectTrait for Rect<'a> {
    type T = f64;
    type ItemType<'b> = (Self::T, Self::T) where Self: 'b;

    fn lower(&self) -> Self::ItemType<'_> {
        let minx = self.values[self.geom_index * 4];
        let miny = self.values[self.geom_index * 4 + 1];
        (minx, miny)
    }

    fn upper(&self) -> Self::ItemType<'_> {
        let maxx = self.values[self.geom_index * 4 + 2];
        let maxy = self.values[self.geom_index * 4 + 3];
        (maxx, maxy)
    }
}

impl From<Rect<'_>> for geo::Rect {
    fn from(value: Rect<'_>) -> Self {
        (&value).into()
    }
}

impl From<&Rect<'_>> for geo::Rect {
    fn from(value: &Rect<'_>) -> Self {
        let lower: geo::Coord = value.lower().into();
        let upper: geo::Coord = value.upper().into();
        geo::Rect::new(lower, upper)
    }
}

impl From<Rect<'_>> for geo::Geometry {
    fn from(value: Rect<'_>) -> Self {
        geo::Geometry::Rect(value.into())
    }
}

impl RTreeObject for Rect<'_> {
    type Envelope = AABB<[f64; 2]>;

    fn envelope(&self) -> Self::Envelope {
        let minx = self.values[self.geom_index * 4];
        let miny = self.values[self.geom_index * 4 + 1];
        let maxx = self.values[self.geom_index * 4 + 2];
        let maxy = self.values[self.geom_index * 4 + 3];
        AABB::from_corners([minx, miny], [maxx, maxy])
    }
}

impl PartialEq for Rect<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.lower() == other.lower() && self.upper() == other.upper()
    }
}
