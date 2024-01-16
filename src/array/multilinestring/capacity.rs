use std::ops::{Add, AddAssign};

use arrow_array::OffsetSizeTrait;

use crate::array::linestring::LineStringCapacity;
use crate::geo_traits::{LineStringTrait, MultiLineStringTrait};

/// A counter for the buffer sizes of a
/// [`MultiLineStringArray`][crate::array::MultiLineStringArray].
///
/// This can be used to reduce allocations by allocating once for exactly the array size you need.
#[derive(Debug, Clone, Copy)]
pub struct MultiLineStringCapacity {
    pub(crate) coord_capacity: usize,
    pub(crate) ring_capacity: usize,
    pub(crate) geom_capacity: usize,
}

impl MultiLineStringCapacity {
    /// Create a new capacity with known sizes.
    pub fn new(coord_capacity: usize, ring_capacity: usize, geom_capacity: usize) -> Self {
        Self {
            coord_capacity,
            ring_capacity,
            geom_capacity,
        }
    }

    /// Create a new empty capacity.
    pub fn new_empty() -> Self {
        Self::new(0, 0, 0)
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
    pub fn add_line_string(&mut self, maybe_line_string: Option<&impl LineStringTrait>) {
        self.geom_capacity += 1;
        if let Some(line_string) = maybe_line_string {
            // A single line string
            self.ring_capacity += 1;
            self.coord_capacity += line_string.num_coords();
        }
    }

    #[inline]
    pub fn add_multi_line_string(&mut self, multi_line_string: Option<&impl MultiLineStringTrait>) {
        self.geom_capacity += 1;
        if let Some(multi_line_string) = multi_line_string {
            // Total number of rings in this polygon
            let num_line_strings = multi_line_string.num_lines();
            self.ring_capacity += num_line_strings;

            for line_string in multi_line_string.lines() {
                self.coord_capacity += line_string.num_coords();
            }
        }
    }

    pub fn add_line_string_capacity(&mut self, line_string_capacity: LineStringCapacity) {
        self.coord_capacity += line_string_capacity.coord_capacity();
        self.ring_capacity += line_string_capacity.geom_capacity();
        self.geom_capacity += line_string_capacity.geom_capacity();
    }

    pub fn from_multi_line_strings<'a>(
        geoms: impl Iterator<Item = Option<&'a (impl MultiLineStringTrait + 'a)>>,
    ) -> Self {
        let mut counter = Self::new_empty();
        for maybe_multi_line_string in geoms.into_iter() {
            counter.add_multi_line_string(maybe_multi_line_string);
        }
        counter
    }

    /// The number of bytes an array with this capacity would occupy.
    pub fn num_bytes<O: OffsetSizeTrait>(&self) -> usize {
        let offsets_byte_width = if O::IS_LARGE { 8 } else { 4 };
        let num_offsets = self.geom_capacity + self.ring_capacity;
        (offsets_byte_width * num_offsets) + (self.coord_capacity * 2 * 8)
    }
}

impl Default for MultiLineStringCapacity {
    fn default() -> Self {
        Self::new_empty()
    }
}

impl Add for MultiLineStringCapacity {
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        let coord_capacity = self.coord_capacity + rhs.coord_capacity;
        let ring_capacity = self.ring_capacity + rhs.ring_capacity;
        let geom_capacity = self.geom_capacity + rhs.geom_capacity;
        Self::new(coord_capacity, ring_capacity, geom_capacity)
    }
}

impl AddAssign for MultiLineStringCapacity {
    fn add_assign(&mut self, rhs: Self) {
        self.coord_capacity += rhs.coord_capacity;
        self.ring_capacity += rhs.ring_capacity;
        self.geom_capacity += rhs.geom_capacity;
    }
}

impl AddAssign<LineStringCapacity> for MultiLineStringCapacity {
    fn add_assign(&mut self, rhs: LineStringCapacity) {
        self.coord_capacity += rhs.coord_capacity();
        self.ring_capacity += rhs.geom_capacity();
        self.geom_capacity += rhs.geom_capacity();
    }
}
