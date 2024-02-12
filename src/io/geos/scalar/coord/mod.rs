use crate::geo_traits::CoordTrait;

mod combined;
mod interleaved;
mod separated;

#[derive(Clone)]
pub struct GEOSConstCoord<'a> {
    pub(crate) coords: geos::CoordSeq<'a>,
    pub(crate) geom_index: usize,
}

impl<'a> CoordTrait for GEOSConstCoord<'a> {
    type T = f64;

    fn x(&self) -> Self::T {
        self.coords.get_x(self.geom_index).unwrap()
    }

    fn y(&self) -> Self::T {
        self.coords.get_y(self.geom_index).unwrap()
    }
}
