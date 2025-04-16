use geoarrow_array::array::MultiLineStringArray;
use geoarrow_array::builder::MultiLineStringBuilder;
use geoarrow_array::error::Result;
use geoarrow_schema::MultiLineStringType;

use crate::import::array::FromGEOS;
use crate::import::scalar::GEOSMultiLineString;

impl FromGEOS for MultiLineStringBuilder {
    type GeoArrowType = MultiLineStringType;

    fn from_geos(
        geoms: impl IntoIterator<Item = Option<geos::Geometry>>,
        typ: Self::GeoArrowType,
    ) -> geoarrow_array::error::Result<Self> {
        let geoms = geoms
            .into_iter()
            .map(|geom| geom.map(GEOSMultiLineString::try_new).transpose())
            .collect::<Result<Vec<_>>>()?;
        Ok(Self::from_nullable_multi_line_strings(&geoms, typ))
    }
}

impl FromGEOS for MultiLineStringArray {
    type GeoArrowType = MultiLineStringType;

    fn from_geos(
        geoms: impl IntoIterator<Item = Option<geos::Geometry>>,
        typ: Self::GeoArrowType,
    ) -> Result<Self> {
        Ok(MultiLineStringBuilder::from_geos(geoms, typ)?.finish())
    }
}

// #[allow(unused_imports)]
// #[cfg(test)]
// mod test {
//     use super::*;
//     use crate::test::multilinestring::ml_array;
//     use crate::trait_::{ArrayAccessor, NativeScalar};

//     #[test]
//     fn geos_round_trip() {
//         let arr = ml_array();
//         let geos_geoms: Vec<Option<geos::Geometry>> = arr
//             .iter()
//             .map(|opt_x| opt_x.map(|x| x.to_geos().unwrap()))
//             .collect();
//         let round_trip = MultiLineStringArray::from_geos(geos_geoms, Dimension::XY).unwrap();
//         assert_eq!(arr, round_trip);
//     }
// }
