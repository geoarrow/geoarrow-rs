use geoarrow_array::array::MultiPointArray;
use geoarrow_array::builder::MultiPointBuilder;
use geoarrow_schema::MultiPointType;
use geoarrow_schema::error::GeoArrowResult;

use crate::import::array::FromGEOS;
use crate::import::scalar::GEOSMultiPoint;

impl FromGEOS for MultiPointBuilder {
    type GeoArrowType = MultiPointType;

    fn from_geos(
        geoms: impl IntoIterator<Item = Option<geos::Geometry>>,
        typ: Self::GeoArrowType,
    ) -> GeoArrowResult<Self> {
        let geoms = geoms
            .into_iter()
            .map(|geom| geom.map(GEOSMultiPoint::try_new).transpose())
            .collect::<GeoArrowResult<Vec<_>>>()?;
        Ok(Self::from_nullable_multi_points(&geoms, typ))
    }
}

impl FromGEOS for MultiPointArray {
    type GeoArrowType = MultiPointType;

    fn from_geos(
        geoms: impl IntoIterator<Item = Option<geos::Geometry>>,
        typ: Self::GeoArrowType,
    ) -> GeoArrowResult<Self> {
        Ok(MultiPointBuilder::from_geos(geoms, typ)?.finish())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::export::to_geos_geometry;

    use geoarrow_array::test::multipoint::array;
    use geoarrow_array::{GeoArrowArrayAccessor, IntoArrow};
    use geoarrow_schema::{CoordType, Dimension};

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
                    MultiPointArray::from_geos(geos_geoms, arr.ext_type().clone()).unwrap();
                assert_eq!(arr, round_trip);
            }
        }
    }
}
