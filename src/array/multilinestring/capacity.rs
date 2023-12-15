use crate::array::linestring::LineStringCapacity;
use crate::geo_traits::{LineStringTrait, MultiLineStringTrait};

#[derive(Debug, Clone, Copy)]
pub struct MultiLineStringCapacity {
    pub(crate) coord_capacity: usize,
    pub(crate) ring_capacity: usize,
    pub(crate) geom_capacity: usize,
}

impl MultiLineStringCapacity {
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

    pub fn add_line_string<'a>(
        &mut self,
        maybe_line_string: Option<&'a (impl LineStringTrait + 'a)>,
    ) {
        self.geom_capacity += 1;
        if let Some(line_string) = maybe_line_string {
            // A single line string
            self.ring_capacity += 1;
            self.coord_capacity += line_string.num_coords();
        }
    }

    pub fn add_multi_line_string<'a>(
        &mut self,
        multi_line_string: Option<&'a (impl MultiLineStringTrait + 'a)>,
    ) {
        self.geom_capacity += 1;
        if let Some(multi_line_string) = multi_line_string {
            // Total number of rings in this polygon
            let num_line_strings = multi_line_string.num_lines();
            self.ring_capacity += num_line_strings;

            for line_string_idx in 0..num_line_strings {
                let line_string = multi_line_string.line(line_string_idx).unwrap();
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
}

impl Default for MultiLineStringCapacity {
    fn default() -> Self {
        Self::new_empty()
    }
}
