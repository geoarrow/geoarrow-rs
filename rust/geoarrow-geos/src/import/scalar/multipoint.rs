use geo_traits::MultiPointTrait;
use geoarrow_schema::error::{GeoArrowError, GeoArrowResult};
use geos::{Geom, GeometryTypes};

use crate::import::scalar::point::GEOSConstPoint;

pub struct GEOSMultiPoint(pub(crate) geos::Geometry);

impl GEOSMultiPoint {
    pub fn new_unchecked(geom: geos::Geometry) -> Self {
        Self(geom)
    }

    #[allow(dead_code)]
    pub fn try_new(geom: geos::Geometry) -> GeoArrowResult<Self> {
        if matches!(geom.geometry_type(), GeometryTypes::MultiPoint) {
            Ok(Self(geom))
        } else {
            Err(GeoArrowError::General(
                "Geometry type must be multi point".to_string(),
            ))
        }
    }

    pub fn num_points(&self) -> usize {
        self.0.get_num_geometries().unwrap()
    }

    pub(crate) fn dimension(&self) -> geo_traits::Dimensions {
        match self.0.get_coordinate_dimension().unwrap() {
            geos::Dimensions::TwoD => geo_traits::Dimensions::Xy,
            geos::Dimensions::ThreeD => geo_traits::Dimensions::Xyz,
            geos::Dimensions::Other(other) => panic!("Other dimensions not supported {other}"),
        }
    }
}

impl MultiPointTrait for GEOSMultiPoint {
    type InnerPointType<'a>
        = GEOSConstPoint<'a>
    where
        Self: 'a;

    fn num_points(&self) -> usize {
        self.0.get_num_geometries().unwrap()
    }

    unsafe fn point_unchecked(&self, i: usize) -> Self::InnerPointType<'_> {
        let point = self.0.get_geometry_n(i).unwrap();
        GEOSConstPoint::new_unchecked(point)
    }
}

impl MultiPointTrait for &GEOSMultiPoint {
    type InnerPointType<'a>
        = GEOSConstPoint<'a>
    where
        Self: 'a;

    fn num_points(&self) -> usize {
        self.0.get_num_geometries().unwrap()
    }

    unsafe fn point_unchecked(&self, i: usize) -> Self::InnerPointType<'_> {
        let point = self.0.get_geometry_n(i).unwrap();
        GEOSConstPoint::new_unchecked(point)
    }
}
