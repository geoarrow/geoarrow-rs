use crate::error::{GeoArrowError, Result};
use crate::io::geos::scalar::coord::GEOSConstCoord;
use geo_traits::LineStringTrait;
use geos::{Geom, GeometryTypes};

pub struct GEOSConstLinearRing<'a>(pub(crate) geos::ConstGeometry<'a>);

impl<'a> GEOSConstLinearRing<'a> {
    pub fn new_unchecked(geom: geos::ConstGeometry<'a>) -> Self {
        Self(geom)
    }

    #[allow(dead_code)]
    pub fn try_new(geom: geos::ConstGeometry<'a>) -> Result<Self> {
        if matches!(geom.geometry_type(), GeometryTypes::LinearRing) {
            Ok(Self(geom))
        } else {
            Err(GeoArrowError::General(
                "Geometry type must be linear ring".to_string(),
            ))
        }
    }
}

impl<'a> LineStringTrait for GEOSConstLinearRing<'a> {
    type T = f64;
    type CoordType<'c> = GEOSConstCoord where Self: 'c;

    fn dim(&self) -> geo_traits::Dimensions {
        match self.0.get_coordinate_dimension().unwrap() {
            geos::Dimensions::TwoD => geo_traits::Dimensions::Xy,
            geos::Dimensions::ThreeD => geo_traits::Dimensions::Xyz,
            geos::Dimensions::Other(other) => panic!("Other dimensions not supported {other}"),
        }
    }

    fn num_coords(&self) -> usize {
        self.0.get_num_coordinates().unwrap()
    }

    unsafe fn coord_unchecked(&self, i: usize) -> Self::CoordType<'_> {
        let seq = self.0.get_coord_seq().unwrap();
        GEOSConstCoord {
            coords: seq,
            geom_index: i,
            dim: self.dim(),
        }
    }
}
