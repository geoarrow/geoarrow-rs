use crate::error::{GeoArrowError, Result};
use crate::geo_traits::MultiPointTrait;
use crate::io::geos::scalar::GEOSConstPoint;
use crate::scalar::MultiPoint;
use geos::{Geom, GeometryTypes};

impl<'a, const D: usize> TryFrom<&'a MultiPoint<'_, D>> for geos::Geometry {
    type Error = geos::Error;

    fn try_from(value: &'a MultiPoint<'_, D>) -> std::result::Result<geos::Geometry, geos::Error> {
        geos::Geometry::create_multipoint(
            value
                .points()
                .map(|point| (&point).try_into())
                .collect::<std::result::Result<Vec<_>, geos::Error>>()?,
        )
    }
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
    type ItemType<'a> = GEOSConstPoint<'a> where Self: 'a;

    fn dim(&self) -> usize {
        match self.0.get_coordinate_dimension().unwrap() {
            geos::Dimensions::TwoD => 2,
            geos::Dimensions::ThreeD => 3,
            geos::Dimensions::Other(other) => panic!("Other dimensions not supported {other}"),
        }
    }

    fn num_points(&self) -> usize {
        self.0.get_num_geometries().unwrap()
    }

    unsafe fn point_unchecked(&self, i: usize) -> Self::ItemType<'_> {
        let point = self.0.get_geometry_n(i).unwrap();
        GEOSConstPoint::new_unchecked(point)
    }
}

impl MultiPointTrait for &GEOSMultiPoint {
    type T = f64;
    type ItemType<'a> = GEOSConstPoint<'a> where Self: 'a;

    fn dim(&self) -> usize {
        match self.0.get_coordinate_dimension().unwrap() {
            geos::Dimensions::TwoD => 2,
            geos::Dimensions::ThreeD => 3,
            geos::Dimensions::Other(other) => panic!("Other dimensions not supported {other}"),
        }
    }

    fn num_points(&self) -> usize {
        self.0.get_num_geometries().unwrap()
    }

    unsafe fn point_unchecked(&self, i: usize) -> Self::ItemType<'_> {
        let point = self.0.get_geometry_n(i).unwrap();
        GEOSConstPoint::new_unchecked(point)
    }
}
