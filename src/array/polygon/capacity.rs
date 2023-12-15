use crate::geo_traits::{LineStringTrait, PolygonTrait, RectTrait};

#[derive(Debug, Clone, Copy)]
pub struct PolygonCapacity {
    pub(crate) coord_capacity: usize,
    pub(crate) ring_capacity: usize,
    pub(crate) geom_capacity: usize,
}

impl PolygonCapacity {
    pub fn new(coord_capacity: usize, ring_capacity: usize, geom_capacity: usize) -> Self {
        Self {
            coord_capacity,
            ring_capacity,
            geom_capacity,
        }
    }

    pub fn new_empty() -> Self {
        Self::new(0, 0, 0)
    }

    pub fn is_empty(&self) -> bool {
        self.coord_capacity == 0 && self.ring_capacity == 0 && self.geom_capacity == 0
    }

    pub fn coord_capacity(&self) -> usize {
        self.coord_capacity
    }

    pub fn ring_capacity(&self) -> usize {
        self.ring_capacity
    }

    pub fn geom_capacity(&self) -> usize {
        self.geom_capacity
    }

    pub fn add_polygon<'a>(&mut self, polygon: Option<&'a (impl PolygonTrait + 'a)>) {
        self.geom_capacity += 1;
        if let Some(polygon) = polygon {
            // Total number of rings in this polygon
            let num_interiors = polygon.num_interiors();
            self.ring_capacity += num_interiors + 1;

            // Number of coords for each ring
            if let Some(exterior) = polygon.exterior() {
                self.coord_capacity += exterior.num_coords();
            }

            for int_ring_idx in 0..polygon.num_interiors() {
                let int_ring = polygon.interior(int_ring_idx).unwrap();
                self.coord_capacity += int_ring.num_coords();
            }
        }
    }

    pub fn add_rect<'a>(&mut self, rect: Option<&'a (impl RectTrait + 'a)>) {
        self.geom_capacity += 1;
        if let Some(_rect) = rect {
            // A rect is a simple polygon with only one ring
            self.ring_capacity += 1;
            // A rect is a closed polygon with 5 coordinates
            self.coord_capacity += 5;
        }
    }

    pub fn from_polygons<'a>(
        geoms: impl Iterator<Item = Option<&'a (impl PolygonTrait + 'a)>>,
    ) -> Self {
        let mut counter = Self::new_empty();
        for maybe_polygon in geoms.into_iter() {
            counter.add_polygon(maybe_polygon);
        }
        counter
    }

    pub fn from_rects<'a>(geoms: impl Iterator<Item = Option<&'a (impl RectTrait + 'a)>>) -> Self {
        let mut counter = Self::new_empty();
        for maybe_rect in geoms.into_iter() {
            counter.add_rect(maybe_rect);
        }
        counter
    }
}

impl Default for PolygonCapacity {
    fn default() -> Self {
        Self::new_empty()
    }
}
