use geoarrow_array::array::GeometryArray;
use geoarrow_array::builder::GeometryBuilder;
use geoarrow_array::error::Result;
use geoarrow_schema::GeometryType;

use crate::import::array::FromGEOS;
use crate::import::scalar::GEOSGeometry;

impl FromGEOS for GeometryBuilder {
    type GeoArrowType = GeometryType;

    fn from_geos(
        geoms: impl IntoIterator<Item = Option<geos::Geometry>>,
        typ: Self::GeoArrowType,
    ) -> geoarrow_array::error::Result<Self> {
        let geoms = geoms
            .into_iter()
            .map(|geom| geom.map(GEOSGeometry::new))
            .collect::<Vec<_>>();
        Self::from_nullable_geometries(&geoms, typ)
    }
}

impl FromGEOS for GeometryArray {
    type GeoArrowType = GeometryType;

    fn from_geos(
        geoms: impl IntoIterator<Item = Option<geos::Geometry>>,
        typ: Self::GeoArrowType,
    ) -> Result<Self> {
        Ok(GeometryBuilder::from_geos(geoms, typ)?.finish())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::export::to_geos_geometry;

    use geoarrow_array::test::geometry::array;
    use geoarrow_array::{GeoArrowArrayAccessor, IntoArrow};
    use geoarrow_schema::CoordType;

    #[ignore = "GEOS doesn't support XYM, XYZM; need to add option to only construct specific dimensions in geometry test array"]
    #[test]
    fn geos_round_trip() {
        for coord_type in [CoordType::Interleaved, CoordType::Separated] {
            let arr = array(coord_type, false);

            let geos_geoms = arr
                .iter()
                .map(|opt_x| opt_x.map(|x| to_geos_geometry(&x.unwrap()).unwrap()))
                .collect::<Vec<_>>();
            let round_trip = GeometryArray::from_geos(geos_geoms, arr.ext_type().clone()).unwrap();
            assert_eq!(arr, round_trip);
        }
    }
}
