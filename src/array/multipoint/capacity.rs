use std::ops::Add;

use arrow_array::OffsetSizeTrait;

use crate::error::{GeoArrowError, Result};
use crate::geo_traits::{GeometryTrait, GeometryType, MultiPointTrait, PointTrait};

/// A counter for the buffer sizes of a [`MultiPointArray`][crate::array::MultiPointArray].
///
/// This can be used to reduce allocations by allocating once for exactly the array size you need.
#[derive(Debug, Clone, Copy)]
pub struct MultiPointCapacity {
    pub(crate) coord_capacity: usize,
    pub(crate) geom_capacity: usize,
}

impl MultiPointCapacity {
    /// Create a new capacity with known sizes.
    pub fn new(coord_capacity: usize, geom_capacity: usize) -> Self {
        Self {
            coord_capacity,
            geom_capacity,
        }
    }

    /// Create a new empty capacity.
    pub fn new_empty() -> Self {
        Self::new(0, 0)
    }

    /// Return `true` if the capacity is empty.
    pub fn is_empty(&self) -> bool {
        self.coord_capacity == 0 && self.geom_capacity == 0
    }

    #[inline]
    pub fn add_point(&mut self, point: Option<&impl PointTrait>) {
        self.geom_capacity += 1;
        if let Some(point) = point {
            self.add_valid_point(point)
        }
    }

    #[inline]
    fn add_valid_point(&mut self, _point: &impl PointTrait) {
        self.coord_capacity += 1;
    }

    #[inline]
    pub fn add_multi_point(&mut self, maybe_multi_point: Option<&impl MultiPointTrait>) {
        self.geom_capacity += 1;

        if let Some(multi_point) = maybe_multi_point {
            self.add_valid_multi_point(multi_point);
        }
    }

    #[inline]
    fn add_valid_multi_point(&mut self, multi_point: &impl MultiPointTrait) {
        self.coord_capacity += multi_point.num_points();
    }

    #[inline]
    pub fn add_geometry(&mut self, value: Option<&impl GeometryTrait>) -> Result<()> {
        self.geom_capacity += 1;

        if let Some(g) = value {
            match g.as_type() {
                GeometryType::Point(p) => self.add_valid_point(p),
                GeometryType::MultiPoint(p) => self.add_valid_multi_point(p),
                _ => return Err(GeoArrowError::General("incorrect type".to_string())),
            }
        };
        Ok(())
    }

    pub fn add_point_capacity(&mut self, point_capacity: usize) {
        self.coord_capacity += point_capacity;
        self.geom_capacity += point_capacity;
    }

    pub fn coord_capacity(&self) -> usize {
        self.coord_capacity
    }

    pub fn geom_capacity(&self) -> usize {
        self.geom_capacity
    }

    pub fn from_multi_points<'a>(
        geoms: impl Iterator<Item = Option<&'a (impl MultiPointTrait + 'a)>>,
    ) -> Self {
        let mut counter = Self::new_empty();

        for maybe_line_string in geoms.into_iter() {
            counter.add_multi_point(maybe_line_string);
        }

        counter
    }

    /// The number of bytes an array with this capacity would occupy.
    pub fn num_bytes<O: OffsetSizeTrait>(&self) -> usize {
        let offsets_byte_width = if O::IS_LARGE { 8 } else { 4 };
        let num_offsets = self.geom_capacity;
        (offsets_byte_width * num_offsets) + (self.coord_capacity * 2 * 8)
    }
}

impl Default for MultiPointCapacity {
    fn default() -> Self {
        Self::new_empty()
    }
}

impl Add for MultiPointCapacity {
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        let coord_capacity = self.coord_capacity + rhs.coord_capacity;
        let geom_capacity = self.geom_capacity + rhs.geom_capacity;
        Self::new(coord_capacity, geom_capacity)
    }
}
