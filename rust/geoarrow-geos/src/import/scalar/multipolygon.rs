use geo_traits::MultiPolygonTrait;
use geoarrow_array::error::{GeoArrowError, Result};
use geos::{Geom, GeometryTypes};

use crate::import::scalar::polygon::GEOSConstPolygon;

pub struct GEOSMultiPolygon(pub(crate) geos::Geometry);

impl GEOSMultiPolygon {
    pub fn new_unchecked(geom: geos::Geometry) -> Self {
        Self(geom)
    }

    #[allow(dead_code)]
    pub fn try_new(geom: geos::Geometry) -> Result<Self> {
        if matches!(geom.geometry_type(), GeometryTypes::MultiPolygon) {
            Ok(Self(geom))
        } else {
            Err(GeoArrowError::General(
                "Geometry type must be multi polygon".to_string(),
            ))
        }
    }
}

impl MultiPolygonTrait for GEOSMultiPolygon {
    type T = f64;
    type PolygonType<'a>
        = GEOSConstPolygon<'a>
    where
        Self: 'a;

    fn dim(&self) -> geo_traits::Dimensions {
        match self.0.get_coordinate_dimension().unwrap() {
            geos::Dimensions::TwoD => geo_traits::Dimensions::Xy,
            geos::Dimensions::ThreeD => geo_traits::Dimensions::Xyz,
            geos::Dimensions::Other(other) => panic!("Other dimensions not supported {other}"),
        }
    }

    fn num_polygons(&self) -> usize {
        self.0.get_num_geometries().unwrap()
    }

    unsafe fn polygon_unchecked(&self, i: usize) -> Self::PolygonType<'_> {
        GEOSConstPolygon::new_unchecked(self.0.get_geometry_n(i).unwrap())
    }
}
