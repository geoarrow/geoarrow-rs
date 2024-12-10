use crate::error::{GeoArrowError, Result};
use crate::io::geos::scalar::point::to_geos_point;
use crate::io::geos::scalar::GEOSConstPoint;
use crate::scalar::MultiPoint;
use geo_traits::MultiPointTrait;
use geos::{Geom, GeometryTypes};

impl<'a> TryFrom<&'a MultiPoint<'_>> for geos::Geometry {
    type Error = geos::Error;

    fn try_from(value: &'a MultiPoint<'_>) -> std::result::Result<geos::Geometry, geos::Error> {
        to_geos_multi_point(value)
    }
}

pub(crate) fn to_geos_multi_point(
    multi_point: &impl MultiPointTrait<T = f64>,
) -> std::result::Result<geos::Geometry, geos::Error> {
    geos::Geometry::create_multipoint(
        multi_point
            .points()
            .map(|point| to_geos_point(&point))
            .collect::<std::result::Result<Vec<_>, geos::Error>>()?,
    )
}

#[derive(Clone)]
pub struct GEOSMultiPoint(pub(crate) geos::Geometry);

impl GEOSMultiPoint {
    pub fn new_unchecked(geom: geos::Geometry) -> Self {
        Self(geom)
    }

    #[allow(dead_code)]
    pub fn try_new(geom: geos::Geometry) -> Result<Self> {
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
}

impl MultiPointTrait for GEOSMultiPoint {
    type T = f64;
    type PointType<'a>
        = GEOSConstPoint<'a>
    where
        Self: 'a;

    fn dim(&self) -> geo_traits::Dimensions {
        match self.0.get_coordinate_dimension().unwrap() {
            geos::Dimensions::TwoD => geo_traits::Dimensions::Xy,
            geos::Dimensions::ThreeD => geo_traits::Dimensions::Xyz,
            geos::Dimensions::Other(other) => panic!("Other dimensions not supported {other}"),
        }
    }

    fn num_points(&self) -> usize {
        self.0.get_num_geometries().unwrap()
    }

    unsafe fn point_unchecked(&self, i: usize) -> Self::PointType<'_> {
        let point = self.0.get_geometry_n(i).unwrap();
        GEOSConstPoint::new_unchecked(point)
    }
}

impl MultiPointTrait for &GEOSMultiPoint {
    type T = f64;
    type PointType<'a>
        = GEOSConstPoint<'a>
    where
        Self: 'a;

    fn dim(&self) -> geo_traits::Dimensions {
        match self.0.get_coordinate_dimension().unwrap() {
            geos::Dimensions::TwoD => geo_traits::Dimensions::Xy,
            geos::Dimensions::ThreeD => geo_traits::Dimensions::Xyz,
            geos::Dimensions::Other(other) => panic!("Other dimensions not supported {other}"),
        }
    }

    fn num_points(&self) -> usize {
        self.0.get_num_geometries().unwrap()
    }

    unsafe fn point_unchecked(&self, i: usize) -> Self::PointType<'_> {
        let point = self.0.get_geometry_n(i).unwrap();
        GEOSConstPoint::new_unchecked(point)
    }
}
