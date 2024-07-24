use std::collections::HashSet;
use std::ops::{Add, AddAssign};

use arrow_array::OffsetSizeTrait;

use crate::geo_traits::{LineStringTrait, PolygonTrait, RectTrait};

/// A counter for the buffer sizes of a [`PolygonArray`][crate::array::PolygonArray].
///
/// This can be used to reduce allocations by allocating once for exactly the array size you need.
#[derive(Debug, Clone)]
pub struct PolygonCapacity {
    pub(crate) coord_capacity: usize,
    pub(crate) ring_capacity: usize,
    pub(crate) geom_capacity: usize,
    pub(crate) dimensions: HashSet<usize>,
}

impl PolygonCapacity {
    /// Create a new capacity with known sizes.
    pub fn new(
        coord_capacity: usize,
        ring_capacity: usize,
        geom_capacity: usize,
        dimensions: HashSet<usize>,
    ) -> Self {
        Self {
            coord_capacity,
            ring_capacity,
            geom_capacity,
            dimensions,
        }
    }

    /// Create a new empty capacity.
    pub fn new_empty() -> Self {
        Self::new(0, 0, 0, HashSet::new())
    }

    /// Return `true` if the capacity is empty.
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

    #[inline]
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

            for int_ring in polygon.interiors() {
                self.coord_capacity += int_ring.num_coords();
            }

            self.dimensions.insert(polygon.dim());
        }
    }

    #[inline]
    pub fn add_rect<'a>(&mut self, rect: Option<&'a (impl RectTrait + 'a)>) {
        self.geom_capacity += 1;
        if let Some(r) = rect {
            // A rect is a simple polygon with only one ring
            self.ring_capacity += 1;
            // A rect is a closed polygon with 5 coordinates
            self.coord_capacity += 5;
            self.dimensions.insert(r.dim());
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

    /// The number of bytes an array with this capacity would occupy.
    pub fn num_bytes<O: OffsetSizeTrait>(&self, dim: usize) -> usize {
        let offsets_byte_width = if O::IS_LARGE { 8 } else { 4 };
        let num_offsets = self.geom_capacity + self.ring_capacity;
        (offsets_byte_width * num_offsets) + (self.coord_capacity * dim * 8)
    }
}

impl Default for PolygonCapacity {
    fn default() -> Self {
        Self::new_empty()
    }
}

impl Add for PolygonCapacity {
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        let mut new = self.clone();
        new += rhs;
        new
    }
}

impl AddAssign for PolygonCapacity {
    fn add_assign(&mut self, rhs: Self) {
        self.coord_capacity += rhs.coord_capacity();
        self.ring_capacity += rhs.ring_capacity();
        self.geom_capacity += rhs.geom_capacity();
        self.dimensions.extend(rhs.dimensions);
    }
}
