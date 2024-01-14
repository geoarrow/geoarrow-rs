use crate::error::{GeoArrowError, Result};
use crate::geo_traits::LineStringTrait;
use crate::io::geos::scalar::coord::GEOSConstCoord;
use geos::{Geom, GeometryTypes};

pub struct GEOSConstLinearRing<'a, 'b>(pub(crate) geos::ConstGeometry<'a, 'b>);

impl<'a, 'b> GEOSConstLinearRing<'a, 'b> {
    pub fn new_unchecked(geom: geos::ConstGeometry<'a, 'b>) -> Self {
        Self(geom)
    }

    #[allow(dead_code)]
    pub fn try_new(geom: geos::ConstGeometry<'a, 'b>) -> Result<Self> {
        if matches!(geom.geometry_type(), GeometryTypes::LinearRing) {
            Ok(Self(geom))
        } else {
            Err(GeoArrowError::General(
                "Geometry type must be linear ring".to_string(),
            ))
        }
    }
}

impl<'a, 'b> LineStringTrait for GEOSConstLinearRing<'a, 'b> {
    type T = f64;
    type ItemType<'c> = GEOSConstCoord<'a> where Self: 'c;

    fn num_coords(&self) -> usize {
        self.0.get_num_coordinates().unwrap()
    }

    unsafe fn coord_unchecked(&self, i: usize) -> Self::ItemType<'_> {
        let seq = self.0.get_coord_seq().unwrap();
        GEOSConstCoord {
            coords: seq,
            geom_index: i,
        }
    }
}
