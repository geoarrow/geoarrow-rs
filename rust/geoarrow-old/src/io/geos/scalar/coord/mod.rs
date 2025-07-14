use geo_traits::CoordTrait;

mod combined;
mod interleaved;
mod separated;

pub(crate) use combined::{coord_to_geos, coords_to_geos};

#[derive(Clone)]
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

pub(crate) fn dims_to_geos(dim: geo_traits::Dimensions) -> geos::CoordDimensions {
    match dim {
        geo_traits::Dimensions::Xy | geo_traits::Dimensions::Unknown(2) => {
            geos::CoordDimensions::TwoD
        }
        geo_traits::Dimensions::Xyz | geo_traits::Dimensions::Unknown(3) => {
            geos::CoordDimensions::ThreeD
        }
        _ => panic!("Invalid coord dimension for GEOS: {:?}", dim),
    }
}
