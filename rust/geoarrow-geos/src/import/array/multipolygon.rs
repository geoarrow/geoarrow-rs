use geoarrow_array::array::MultiPolygonArray;
use geoarrow_array::builder::MultiPolygonBuilder;
use geoarrow_schema::MultiPolygonType;
use geoarrow_schema::error::GeoArrowResult;

use crate::import::array::FromGEOS;
use crate::import::scalar::GEOSMultiPolygon;

impl FromGEOS for MultiPolygonBuilder {
    type GeoArrowType = MultiPolygonType;

    fn from_geos(
        geoms: impl IntoIterator<Item = Option<geos::Geometry>>,
        typ: Self::GeoArrowType,
    ) -> GeoArrowResult<Self> {
        let geoms = geoms
            .into_iter()
            .map(|geom| geom.map(GEOSMultiPolygon::try_new).transpose())
            .collect::<GeoArrowResult<Vec<_>>>()?;
        Ok(Self::from_nullable_multi_polygons(&geoms, typ))
    }
}

impl FromGEOS for MultiPolygonArray {
    type GeoArrowType = MultiPolygonType;

    fn from_geos(
        geoms: impl IntoIterator<Item = Option<geos::Geometry>>,
        typ: Self::GeoArrowType,
    ) -> GeoArrowResult<Self> {
        Ok(MultiPolygonBuilder::from_geos(geoms, typ)?.finish())
    }
}
#[cfg(test)]
mod test {
    use geoarrow_array::test::multipolygon::array;
    use geoarrow_array::{GeoArrowArrayAccessor, IntoArrow};
    use geoarrow_schema::{CoordType, Dimension};

    use super::*;
    use crate::export::to_geos_geometry;

    #[test]
    fn geos_round_trip() {
        for coord_type in [CoordType::Interleaved, CoordType::Separated] {
            for dim in [Dimension::XY, Dimension::XYZ] {
                let arr = array(coord_type, dim);

                let geos_geoms = arr
                    .iter()
                    .map(|opt_x| opt_x.map(|x| to_geos_geometry(&x.unwrap()).unwrap()))
                    .collect::<Vec<_>>();
                let round_trip =
                    MultiPolygonArray::from_geos(geos_geoms, arr.extension_type().clone()).unwrap();
                assert_eq!(arr, round_trip);
            }
        }
    }
}
