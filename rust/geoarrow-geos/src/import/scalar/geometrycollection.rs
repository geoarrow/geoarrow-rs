use geo_traits::GeometryCollectionTrait;
use geoarrow_schema::error::{GeoArrowError, GeoArrowResult};
use geos::{Geom, GeometryTypes};

use crate::import::scalar::geometry::GEOSGeometry;

pub struct GEOSGeometryCollection(geos::Geometry);

impl GEOSGeometryCollection {
    pub fn new_unchecked(geom: geos::Geometry) -> Self {
        Self(geom)
    }

    pub fn try_new(geom: geos::Geometry) -> GeoArrowResult<Self> {
        if matches!(geom.geometry_type(), GeometryTypes::GeometryCollection) {
            Ok(Self(geom))
        } else {
            Err(GeoArrowError::IncorrectGeometryType(
                "Geometry type must be geometry collection".to_string(),
            ))
        }
    }

    pub(crate) fn dimension(&self) -> geo_traits::Dimensions {
        match self.0.get_coordinate_dimension().unwrap() {
            geos::Dimensions::TwoD => geo_traits::Dimensions::Xy,
            geos::Dimensions::ThreeD => geo_traits::Dimensions::Unknown(3),
            geos::Dimensions::Other(other) => panic!("Other dimensions not supported {other}"),
        }
    }
}

impl GeometryCollectionTrait for GEOSGeometryCollection {
    type GeometryType<'a> = GEOSGeometry;

    fn num_geometries(&self) -> usize {
        self.0.get_num_geometries().unwrap()
    }

    unsafe fn geometry_unchecked(&self, _i: usize) -> Self::GeometryType<'_> {
        // self.0.get_geometry_n(n)
        todo!("need to have ConstGeometry wrappers too")
    }
}
