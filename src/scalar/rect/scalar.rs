use arrow2::buffer::Buffer;
use rstar::{RTreeObject, AABB};

pub struct Rect<'a> {
    pub values: &'a Buffer<f64>,
    pub geom_index: usize,
}

impl<'a> Rect<'a> {
    pub fn lower(&self) -> (f64, f64) {
        let minx = self.values[self.geom_index * 4];
        let miny = self.values[self.geom_index * 4 + 1];
        (minx, miny)
    }

    pub fn upper(&self) -> (f64, f64) {
        let maxx = self.values[self.geom_index * 4 + 2];
        let maxy = self.values[self.geom_index * 4 + 3];
        (maxx, maxy)
    }
}

impl From<Rect<'_>> for geo::Rect {
    fn from(value: Rect<'_>) -> Self {
        let lower: geo::Coord = value.lower().into();
        let upper: geo::Coord = value.upper().into();
        geo::Rect::new(lower, upper)
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
