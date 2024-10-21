use crate::geo_traits::PointTrait;

mod combined;
mod interleaved;
mod separated;

#[derive(Clone)]
pub struct GEOSConstCoord {
    pub(crate) coords: geos::CoordSeq,
    pub(crate) geom_index: usize,
    pub(crate) dim: crate::geo_traits::Dimension,
}

impl PointTrait for GEOSConstCoord {
    type T = f64;

    fn dim(&self) -> crate::geo_traits::Dimension {
        self.dim
    }

    fn nth_unchecked(&self, n: usize) -> Self::T {
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
