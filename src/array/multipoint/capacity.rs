use crate::geo_traits::{MultiPointTrait, PointTrait};

#[derive(Debug, Clone, Copy)]
pub struct MultiPointCapacity {
    pub(crate) coord_capacity: usize,
    pub(crate) geom_capacity: usize,
}

impl MultiPointCapacity {
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

    pub fn add_point<'a>(&mut self, point: Option<&'a (impl PointTrait + 'a)>) {
        self.geom_capacity += 1;
        if let Some(_point) = point {
            self.coord_capacity += 1;
        }
    }

    pub fn add_multi_point<'a>(
        &mut self,
        maybe_multi_point: Option<&'a (impl MultiPointTrait + 'a)>,
    ) {
        self.geom_capacity += 1;

        if let Some(multi_point) = maybe_multi_point {
            self.coord_capacity += multi_point.num_points();
        }
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
}

impl Default for MultiPointCapacity {
    fn default() -> Self {
        Self::new_empty()
    }
}
