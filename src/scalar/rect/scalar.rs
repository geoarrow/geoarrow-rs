use arrow2::buffer::Buffer;
use rstar::{RTreeObject, AABB};

use crate::geo_traits::RectTrait;
use crate::trait_::GeometryScalarTrait;

#[derive(Debug, Clone)]
pub struct Rect<'a> {
    pub values: &'a Buffer<f64>,
    pub geom_index: usize,
}

impl<'a> GeometryScalarTrait<'a> for Rect<'a> {
    type ScalarGeo = geo::Rect;

    fn to_geo(&self) -> Self::ScalarGeo {
        self.into()
    }
}

impl<'a> RectTrait<'a> for Rect<'a> {
    type T = f64;
    type ItemType = (Self::T, Self::T);

    fn lower(&self) -> Self::ItemType {
        let minx = self.values[self.geom_index * 4];
        let miny = self.values[self.geom_index * 4 + 1];
        (minx, miny)
    }

    fn upper(&self) -> Self::ItemType {
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
