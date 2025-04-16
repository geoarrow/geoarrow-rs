use geoarrow_array::array::PolygonArray;
use geoarrow_array::builder::PolygonBuilder;
use geoarrow_array::error::Result;
use geoarrow_schema::PolygonType;

use crate::import::array::FromGEOS;
use crate::import::scalar::GEOSPolygon;

impl FromGEOS for PolygonBuilder {
    type GeoArrowType = PolygonType;

    fn from_geos(
        geoms: impl IntoIterator<Item = Option<geos::Geometry>>,
        typ: Self::GeoArrowType,
    ) -> geoarrow_array::error::Result<Self> {
        let geoms = geoms
            .into_iter()
            .map(|geom| geom.map(GEOSPolygon::try_new).transpose())
            .collect::<Result<Vec<_>>>()?;
        Ok(Self::from_nullable_polygons(&geoms, typ))
    }
}

impl FromGEOS for PolygonArray {
    type GeoArrowType = PolygonType;

    fn from_geos(
        geoms: impl IntoIterator<Item = Option<geos::Geometry>>,
        typ: Self::GeoArrowType,
    ) -> Result<Self> {
        Ok(PolygonBuilder::from_geos(geoms, typ)?.finish())
    }
}

// #[allow(unused_imports)]
// #[cfg(test)]
// mod test {
//     use super::*;
//     use crate::test::polygon::p_array;
//     use crate::trait_::{ArrayAccessor, NativeScalar};

//     #[test]
//     fn geos_round_trip() {
//         let arr = p_array();
//         let geos_geoms: Vec<Option<geos::Geometry>> = arr
//             .iter()
//             .map(|opt_x| opt_x.map(|x| x.to_geos().unwrap()))
//             .collect();
//         let round_trip = PolygonArray::from_geos(geos_geoms, Dimension::XY).unwrap();
//         assert_eq!(arr, round_trip);
//     }
// }
