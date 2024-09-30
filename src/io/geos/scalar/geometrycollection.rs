use crate::geo_traits::GeometryCollectionTrait;
use crate::io::geos::scalar::GEOSGeometry;
use crate::scalar::GeometryCollection;
use geos::Geom;

impl<'a, const D: usize> TryFrom<&'a GeometryCollection<'_, D>> for geos::Geometry {
    type Error = geos::Error;

    fn try_from(
        value: &'a GeometryCollection<'_, D>,
    ) -> std::result::Result<geos::Geometry, geos::Error> {
        geos::Geometry::create_geometry_collection(
            value
                .geometries()
                .map(|geometry| (&geometry).try_into())
                .collect::<std::result::Result<Vec<_>, geos::Error>>()?,
        )
    }
}

#[derive(Clone)]
pub struct GEOSGeometryCollection(geos::Geometry);

impl GEOSGeometryCollection {
    pub fn new_unchecked(geom: geos::Geometry) -> Self {
        Self(geom)
    }
}

impl GeometryCollectionTrait for GEOSGeometryCollection {
    type T = f64;
    type ItemType<'a> = GEOSGeometry;

    fn dim(&self) -> usize {
        match self.0.get_coordinate_dimension().unwrap() {
            geos::Dimensions::TwoD => 2,
            geos::Dimensions::ThreeD => 3,
            geos::Dimensions::Other(other) => panic!("Other dimensions not supported {other}"),
        }
    }

    fn num_geometries(&self) -> usize {
        self.0.get_num_geometries().unwrap()
    }

    unsafe fn geometry_unchecked(&self, _i: usize) -> Self::ItemType<'_> {
        // self.0.get_geometry_n(n)
        todo!()
    }
}
