use crate::geo_traits::LineStringTrait;

#[derive(Debug, Clone, Copy)]
pub struct LineStringCapacity {
    coord_capacity: usize,
    geom_capacity: usize,
}

impl LineStringCapacity {
    pub fn new(coord_capacity: usize, geom_capacity: usize) -> Self {
        Self {
            coord_capacity,
            geom_capacity,
        }
    }

    pub fn new_empty() -> Self {
        Self::new(0, 0)
    }

    pub fn is_empty(&self) -> bool {
        self.coord_capacity == 0 && self.geom_capacity == 0
    }

    pub fn add_line_string<'a>(
        &mut self,
        maybe_line_string: Option<&'a (impl LineStringTrait + 'a)>,
    ) {
        self.geom_capacity += 1;
        if let Some(line_string) = maybe_line_string {
            self.coord_capacity += line_string.num_coords();
        }
    }

    pub fn coord_capacity(&self) -> usize {
        self.coord_capacity
    }

    pub fn geom_capacity(&self) -> usize {
        self.geom_capacity
    }

    pub fn from_line_strings<'a>(
        geoms: impl Iterator<Item = Option<&'a (impl LineStringTrait + 'a)>>,
    ) -> Self {
        let mut counter = Self::new_empty();

        for maybe_line_string in geoms.into_iter() {
            counter.add_line_string(maybe_line_string);
        }

        counter
    }
}

impl Default for LineStringCapacity {
    fn default() -> Self {
        Self::new_empty()
    }
}
