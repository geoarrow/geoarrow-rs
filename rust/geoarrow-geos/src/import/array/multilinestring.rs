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

#[cfg(test)]
mod test {
    use super::*;
    use crate::export::to_geos_geometry;

    use geoarrow_array::test::multilinestring::array;
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
                    MultiLineStringArray::from_geos(geos_geoms, arr.ext_type().clone()).unwrap();
                assert_eq!(arr, round_trip);
            }
        }
    }
}
