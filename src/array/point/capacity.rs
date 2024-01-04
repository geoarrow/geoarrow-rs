#![allow(dead_code)]

use crate::error::{GeoArrowError, Result};
use crate::geo_traits::{GeometryTrait, GeometryType, PointTrait};
use std::ops::Add;

#[derive(Debug, Clone, Copy)]
pub struct PointCapacity {
    pub(crate) geom_capacity: usize,
}

impl PointCapacity {
    pub fn new(geom_capacity: usize) -> Self {
        Self { geom_capacity }
    }

    pub fn new_empty() -> Self {
        Self::new(0)
    }

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
}

impl Add for PointCapacity {
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        Self::new(self.geom_capacity + rhs.geom_capacity)
    }
}
