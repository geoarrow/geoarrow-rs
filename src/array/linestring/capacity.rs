use std::ops::Add;

use arrow_array::OffsetSizeTrait;

use crate::error::{GeoArrowError, Result};
use crate::geo_traits::{GeometryTrait, GeometryType, LineStringTrait};

/// A counter for the buffer sizes of a [`LineStringArray`][crate::array::LineStringArray].
///
/// This can be used to reduce allocations by allocating once for exactly the array size you need.
#[derive(Debug, Clone, Copy)]
pub struct LineStringCapacity {
    pub(crate) coord_capacity: usize,
    pub(crate) geom_capacity: usize,
}

impl LineStringCapacity {
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

    /// Add a LineString to this capacity counter.
    #[inline]
    pub fn add_line_string(&mut self, maybe_line_string: Option<&impl LineStringTrait>) {
        self.geom_capacity += 1;
        if let Some(line_string) = maybe_line_string {
            self.add_valid_line_string(line_string);
        }
    }

    #[inline]
    fn add_valid_line_string(&mut self, line_string: &impl LineStringTrait) {
        self.coord_capacity += line_string.num_coords();
    }

    #[inline]
    pub fn add_geometry(&mut self, value: Option<&impl GeometryTrait>) -> Result<()> {
        self.geom_capacity += 1;

        if let Some(g) = value {
            match g.as_type() {
                GeometryType::LineString(p) => self.add_valid_line_string(p),
                _ => return Err(GeoArrowError::General("incorrect type".to_string())),
            }
        };
        Ok(())
    }
    pub fn coord_capacity(&self) -> usize {
        self.coord_capacity
    }

    pub fn geom_capacity(&self) -> usize {
        self.geom_capacity
    }

    /// Create a capacity counter from an iterator of LineStrings.
    pub fn from_line_strings<'a>(
        geoms: impl Iterator<Item = Option<&'a (impl LineStringTrait + 'a)>>,
    ) -> Self {
        let mut counter = Self::new_empty();

        for maybe_line_string in geoms.into_iter() {
            counter.add_line_string(maybe_line_string);
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

impl Default for LineStringCapacity {
    fn default() -> Self {
        Self::new_empty()
    }
}

impl Add for LineStringCapacity {
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        let coord_capacity = self.coord_capacity + rhs.coord_capacity;
        let geom_capacity = self.geom_capacity + rhs.geom_capacity;
        Self::new(coord_capacity, geom_capacity)
    }
}
