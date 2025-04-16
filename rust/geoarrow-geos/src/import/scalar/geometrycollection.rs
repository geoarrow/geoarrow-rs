use geo_traits::GeometryCollectionTrait;
use geos::Geom;

use crate::import::scalar::geometry::GEOSGeometry;

pub struct GEOSGeometryCollection(geos::Geometry);

impl GEOSGeometryCollection {
    pub fn new_unchecked(geom: geos::Geometry) -> Self {
        Self(geom)
    }
}

impl GeometryCollectionTrait for GEOSGeometryCollection {
    type T = f64;
    type GeometryType<'a> = GEOSGeometry;

    fn dim(&self) -> geo_traits::Dimensions {
        match self.0.get_coordinate_dimension().unwrap() {
            geos::Dimensions::TwoD => geo_traits::Dimensions::Xy,
            geos::Dimensions::ThreeD => geo_traits::Dimensions::Unknown(3),
            geos::Dimensions::Other(other) => panic!("Other dimensions not supported {other}"),
        }
    }

    fn num_geometries(&self) -> usize {
        self.0.get_num_geometries().unwrap()
    }

    unsafe fn geometry_unchecked(&self, _i: usize) -> Self::GeometryType<'_> {
        // self.0.get_geometry_n(n)
        todo!()
    }
}
