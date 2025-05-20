use geoarrow_array::array::PointArray;
use geoarrow_array::builder::PointBuilder;
use geoarrow_schema::PointType;
use geoarrow_schema::error::GeoArrowResult;

use crate::import::array::FromGEOS;
use crate::import::scalar::GEOSPoint;

impl FromGEOS for PointBuilder {
    type GeoArrowType = PointType;

    fn from_geos(
        geoms: impl IntoIterator<Item = Option<geos::Geometry>>,
        typ: Self::GeoArrowType,
    ) -> GeoArrowResult<Self> {
        let geoms = geoms
            .into_iter()
            .map(|geom| geom.map(GEOSPoint::try_new).transpose())
            .collect::<GeoArrowResult<Vec<_>>>()?;
        Ok(Self::from_nullable_points(
            geoms.iter().map(|x| x.as_ref()),
            typ,
        ))
    }
}

impl FromGEOS for PointArray {
    type GeoArrowType = PointType;

    fn from_geos(
        geoms: impl IntoIterator<Item = Option<geos::Geometry>>,
        typ: Self::GeoArrowType,
    ) -> GeoArrowResult<Self> {
        Ok(PointBuilder::from_geos(geoms, typ)?.finish())
    }
}

#[cfg(test)]
mod test {
    use geoarrow_array::test::point::array;
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
                    PointArray::from_geos(geos_geoms, arr.extension_type().clone()).unwrap();
                assert_eq!(arr, round_trip);
            }
        }
    }
}
