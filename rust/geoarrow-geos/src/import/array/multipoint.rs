use geoarrow_array::array::MultiPointArray;
use geoarrow_array::builder::MultiPointBuilder;
use geoarrow_array::error::Result;
use geoarrow_schema::MultiPointType;

use crate::import::array::FromGEOS;
use crate::import::scalar::GEOSMultiPoint;

impl FromGEOS for MultiPointBuilder {
    type GeoArrowType = MultiPointType;

    fn from_geos(
        geoms: impl IntoIterator<Item = Option<geos::Geometry>>,
        typ: Self::GeoArrowType,
    ) -> geoarrow_array::error::Result<Self> {
        let geoms = geoms
            .into_iter()
            .map(|geom| geom.map(GEOSMultiPoint::try_new).transpose())
            .collect::<Result<Vec<_>>>()?;
        Ok(Self::from_nullable_multi_points(&geoms, typ))
    }
}

impl FromGEOS for MultiPointArray {
    type GeoArrowType = MultiPointType;

    fn from_geos(
        geoms: impl IntoIterator<Item = Option<geos::Geometry>>,
        typ: Self::GeoArrowType,
    ) -> Result<Self> {
        Ok(MultiPointBuilder::from_geos(geoms, typ)?.finish())
    }
}

// #[allow(unused_imports)]
// #[cfg(test)]
// mod test {
//     use super::*;
//     use crate::test::multipoint::mp_array;
//     use crate::trait_::{ArrayAccessor, NativeScalar};

//     #[test]
//     fn geos_round_trip() {
//         let arr = mp_array();
//         let geos_geoms: Vec<Option<geos::Geometry>> = arr
//             .iter()
//             .map(|opt_x| opt_x.map(|x| x.to_geos().unwrap()))
//             .collect();
//         let round_trip = MultiPointArray::from_geos(geos_geoms, Dimension::XY).unwrap();
//         assert_eq!(arr, round_trip);
//     }
// }
