use geoarrow_array::array::LineStringArray;
use geoarrow_array::builder::LineStringBuilder;
use geoarrow_array::error::Result;
use geoarrow_schema::LineStringType;

use crate::import::array::FromGEOS;
use crate::import::scalar::GEOSLineString;

impl FromGEOS for LineStringBuilder {
    type GeoArrowType = LineStringType;

    fn from_geos(
        geoms: impl IntoIterator<Item = Option<geos::Geometry>>,
        typ: Self::GeoArrowType,
    ) -> geoarrow_array::error::Result<Self> {
        let geoms = geoms
            .into_iter()
            .map(|geom| geom.map(GEOSLineString::try_new).transpose())
            .collect::<Result<Vec<_>>>()?;
        Ok(Self::from_nullable_line_strings(&geoms, typ))
    }
}

impl FromGEOS for LineStringArray {
    type GeoArrowType = LineStringType;

    fn from_geos(
        geoms: impl IntoIterator<Item = Option<geos::Geometry>>,
        typ: Self::GeoArrowType,
    ) -> Result<Self> {
        Ok(LineStringBuilder::from_geos(geoms, typ)?.finish())
    }
}

// #[allow(unused_imports)]
// #[cfg(test)]
// mod test {
//     use super::*;
//     use crate::test::linestring::ls_array;
//     use crate::trait_::{ArrayAccessor, NativeScalar};

//     #[test]
//     fn geos_round_trip() {
//         let arr = ls_array();
//         let geos_geoms: Vec<Option<geos::Geometry>> = arr
//             .iter()
//             .map(|opt_x| opt_x.map(|x| x.to_geos().unwrap()))
//             .collect();
//         let round_trip = LineStringArray::from_geos(geos_geoms, Dimension::XY).unwrap();
//         assert_eq!(arr, round_trip);
//     }
// }
