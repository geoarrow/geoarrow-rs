use crate::error::{GeoArrowError, Result};
use crate::io::geos::scalar::GEOSConstPolygon;
use crate::scalar::MultiPolygon;
use geo_traits::MultiPolygonTrait;
use geos::{Geom, GeometryTypes};

impl<'a> TryFrom<&'a MultiPolygon<'_>> for geos::Geometry {
    type Error = geos::Error;

    fn try_from(value: &'a MultiPolygon<'_>) -> std::result::Result<geos::Geometry, geos::Error> {
        geos::Geometry::create_multipolygon(
            value
                .polygons()
                .map(|polygon| (&polygon).try_into())
                .collect::<std::result::Result<Vec<_>, geos::Error>>()?,
        )
    }
}

#[derive(Clone)]
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
