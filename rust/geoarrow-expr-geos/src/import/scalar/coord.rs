use geo_traits::CoordTrait;

pub struct GEOSConstCoord {
    pub(crate) coords: geos::CoordSeq,
    pub(crate) geom_index: usize,
    pub(crate) dim: geo_traits::Dimensions,
}

impl CoordTrait for GEOSConstCoord {
    type T = f64;

    fn dim(&self) -> geo_traits::Dimensions {
        self.dim
    }

    fn nth_or_panic(&self, n: usize) -> Self::T {
        match n {
            0 => self.coords.get_x(self.geom_index).unwrap(),
            1 => self.coords.get_y(self.geom_index).unwrap(),
            2 => self.coords.get_z(self.geom_index).unwrap(),
            _ => panic!(),
        }
    }

    fn x(&self) -> Self::T {
        self.coords.get_x(self.geom_index).unwrap()
    }

    fn y(&self) -> Self::T {
        self.coords.get_y(self.geom_index).unwrap()
    }
}
