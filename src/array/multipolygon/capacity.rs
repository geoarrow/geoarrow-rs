use crate::array::polygon::PolygonCapacity;
use crate::geo_traits::{LineStringTrait, MultiPolygonTrait, PolygonTrait};

#[derive(Debug, Clone, Copy)]
pub struct MultiPolygonCapacity {
    pub(crate) coord_capacity: usize,
    pub(crate) ring_capacity: usize,
    pub(crate) polygon_capacity: usize,
    pub(crate) geom_capacity: usize,
}

impl MultiPolygonCapacity {
    pub fn new(
        coord_capacity: usize,
        ring_capacity: usize,
        polygon_capacity: usize,
        geom_capacity: usize,
    ) -> Self {
        Self {
            coord_capacity,
            ring_capacity,
            polygon_capacity,
            geom_capacity,
        }
    }

    pub fn new_empty() -> Self {
        Self::new(0, 0, 0, 0)
    }

    pub fn is_empty(&self) -> bool {
        self.coord_capacity == 0
            && self.ring_capacity == 0
            && self.polygon_capacity == 0
            && self.geom_capacity == 0
    }

    pub fn coord_capacity(&self) -> usize {
        self.coord_capacity
    }

    pub fn ring_capacity(&self) -> usize {
        self.ring_capacity
    }

    pub fn polygon_capacity(&self) -> usize {
        self.polygon_capacity
    }

    pub fn geom_capacity(&self) -> usize {
        self.geom_capacity
    }

    pub fn add_polygon<'a>(&mut self, polygon: Option<&'a (impl PolygonTrait + 'a)>) {
        self.geom_capacity += 1;
        if let Some(polygon) = polygon {
            // A single polygon
            self.polygon_capacity += 1;

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

    pub fn add_multi_polygon<'a>(
        &mut self,
        multi_polygon: Option<&'a (impl MultiPolygonTrait + 'a)>,
    ) {
        self.geom_capacity += 1;

        if let Some(multi_polygon) = multi_polygon {
            // Total number of polygons in this MultiPolygon
            let num_polygons = multi_polygon.num_polygons();
            self.polygon_capacity += num_polygons;

            for polygon_idx in 0..num_polygons {
                let polygon = multi_polygon.polygon(polygon_idx).unwrap();

                // Total number of rings in this MultiPolygon
                self.ring_capacity += polygon.num_interiors() + 1;

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
    }

    pub fn add_polygon_capacity(&mut self, capacity: PolygonCapacity) {
        // NOTE: I think this will overallocate if there are null values?
        // Because it assumes that every geometry has exactly one polygon, which won't be true if
        // there are null values?
        self.coord_capacity += capacity.coord_capacity();
        self.ring_capacity += capacity.ring_capacity();
        self.polygon_capacity += capacity.geom_capacity();
        self.geom_capacity += capacity.geom_capacity();
    }

    pub fn from_multi_polygons<'a>(
        geoms: impl Iterator<Item = Option<&'a (impl MultiPolygonTrait + 'a)>>,
    ) -> Self {
        let mut counter = Self::new_empty();
        for maybe_multi_polygon in geoms.into_iter() {
            counter.add_multi_polygon(maybe_multi_polygon);
        }
        counter
    }
}

impl Default for MultiPolygonCapacity {
    fn default() -> Self {
        Self::new_empty()
    }
}
