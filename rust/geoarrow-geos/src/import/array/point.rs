use geoarrow_array::array::PointArray;
use geoarrow_array::builder::PointBuilder;
use geoarrow_array::error::Result;
use geoarrow_schema::PointType;

use crate::import::array::FromGEOS;
use crate::import::scalar::GEOSPoint;

impl FromGEOS for PointBuilder {
    type GeoArrowType = PointType;

    fn from_geos(
        geoms: impl IntoIterator<Item = Option<geos::Geometry>>,
        typ: Self::GeoArrowType,
    ) -> geoarrow_array::error::Result<Self> {
        let geoms = geoms
            .into_iter()
            .map(|geom| geom.map(GEOSPoint::try_new).transpose())
            .collect::<Result<Vec<_>>>()?;
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
    ) -> Result<Self> {
        Ok(PointBuilder::from_geos(geoms, typ)?.finish())
    }
}

// #[cfg(test)]
// mod test {
//     use super::*;
//     use crate::test::point::point_array;
//     use crate::trait_::{ArrayAccessor, NativeScalar};

//     #[test]
//     fn geos_round_trip() {
//         let arr = point_array();
//         let geos_geoms: Vec<Option<geos::Geometry>> = arr
//             .iter()
//             .map(|opt_x| opt_x.map(|x| x.to_geos().unwrap()))
//             .collect();
//         let round_trip = PointArray::from_geos(geos_geoms, Dimension::XY).unwrap();
//         assert_eq!(arr, round_trip);
//     }
// }
