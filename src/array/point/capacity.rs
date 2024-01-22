#![allow(dead_code)]

use crate::error::{GeoArrowError, Result};
use crate::geo_traits::{GeometryTrait, GeometryType, PointTrait};
use std::ops::Add;

/// A counter for the buffer sizes of a [`PointArray`][crate::array::PointArray].
///
/// This can be used to reduce allocations by allocating once for exactly the array size you need.
#[derive(Debug, Clone, Copy)]
pub struct PointCapacity {
    pub(crate) geom_capacity: usize,
}

impl PointCapacity {
    /// Create a new capacity with known size.
    pub fn new(geom_capacity: usize) -> Self {
        Self { geom_capacity }
    }

    /// Create a new empty capacity.
    pub fn new_empty() -> Self {
        Self::new(0)
    }

    /// Return `true` if the capacity is empty.
    pub fn is_empty(&self) -> bool {
        self.geom_capacity == 0
    }

    #[inline]
    pub fn add_point(&mut self, _point: Option<&impl PointTrait>) {
        self.geom_capacity += 1;
    }

    #[inline]
    pub fn add_geometry(&mut self, value: Option<&impl GeometryTrait>) -> Result<()> {
        if let Some(g) = value {
            match g.as_type() {
                GeometryType::Point(p) => self.add_point(Some(p)),
                _ => return Err(GeoArrowError::General("incorrect type".to_string())),
            }
        } else {
            self.geom_capacity += 1;
        };
        Ok(())
    }

    /// The number of bytes an array with this capacity would occupy.
    pub fn num_bytes(&self) -> usize {
        self.geom_capacity * 2 * 8
    }
}

impl Add for PointCapacity {
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        Self::new(self.geom_capacity + rhs.geom_capacity)
    }
}
