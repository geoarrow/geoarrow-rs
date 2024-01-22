use std::ops::{Add, AddAssign};

use arrow_array::OffsetSizeTrait;

use crate::array::polygon::PolygonCapacity;
use crate::geo_traits::{LineStringTrait, MultiPolygonTrait, PolygonTrait};

/// A counter for the buffer sizes of a [`MultiPolygonArray`][crate::array::MultiPolygonArray].
///
/// This can be used to reduce allocations by allocating once for exactly the array size you need.
#[derive(Debug, Clone, Copy)]
pub struct MultiPolygonCapacity {
    pub(crate) coord_capacity: usize,
    pub(crate) ring_capacity: usize,
    pub(crate) polygon_capacity: usize,
    pub(crate) geom_capacity: usize,
}

impl MultiPolygonCapacity {
    /// Create a new capacity with known sizes.
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

    /// Create a new empty capacity.
    pub fn new_empty() -> Self {
        Self::new(0, 0, 0, 0)
    }

    /// Return `true` if the capacity is empty.
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

    #[inline]
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

            for int_ring in polygon.interiors() {
                self.coord_capacity += int_ring.num_coords();
            }
        }
    }

    #[inline]
    pub fn add_multi_polygon<'a>(
        &mut self,
        multi_polygon: Option<&'a (impl MultiPolygonTrait + 'a)>,
    ) {
        self.geom_capacity += 1;

        if let Some(multi_polygon) = multi_polygon {
            // Total number of polygons in this MultiPolygon
            let num_polygons = multi_polygon.num_polygons();
            self.polygon_capacity += num_polygons;

            for polygon in multi_polygon.polygons() {
                // Total number of rings in this MultiPolygon
                self.ring_capacity += polygon.num_interiors() + 1;

                // Number of coords for each ring
                if let Some(exterior) = polygon.exterior() {
                    self.coord_capacity += exterior.num_coords();
                }

                for int_ring in polygon.interiors() {
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

    /// The number of bytes an array with this capacity would occupy.
    pub fn num_bytes<O: OffsetSizeTrait>(&self) -> usize {
        let offsets_byte_width = if O::IS_LARGE { 8 } else { 4 };
        let num_offsets = self.geom_capacity + self.polygon_capacity + self.ring_capacity;
        (offsets_byte_width * num_offsets) + (self.coord_capacity * 2 * 8)
    }
}

impl Default for MultiPolygonCapacity {
    fn default() -> Self {
        Self::new_empty()
    }
}

impl Add for MultiPolygonCapacity {
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        let coord_capacity = self.coord_capacity + rhs.coord_capacity;
        let ring_capacity = self.ring_capacity + rhs.ring_capacity;
        let polygon_capacity = self.polygon_capacity + rhs.polygon_capacity;
        let geom_capacity = self.geom_capacity + rhs.geom_capacity;
        Self::new(
            coord_capacity,
            ring_capacity,
            polygon_capacity,
            geom_capacity,
        )
    }
}

impl AddAssign for MultiPolygonCapacity {
    fn add_assign(&mut self, rhs: Self) {
        self.coord_capacity += rhs.coord_capacity;
        self.ring_capacity += rhs.ring_capacity;
        self.polygon_capacity += rhs.polygon_capacity;
        self.geom_capacity += rhs.geom_capacity;
    }
}

impl AddAssign<PolygonCapacity> for MultiPolygonCapacity {
    fn add_assign(&mut self, rhs: PolygonCapacity) {
        self.coord_capacity += rhs.coord_capacity();
        self.ring_capacity += rhs.ring_capacity();
        self.polygon_capacity += rhs.geom_capacity();
        self.geom_capacity += rhs.geom_capacity();
    }
}
