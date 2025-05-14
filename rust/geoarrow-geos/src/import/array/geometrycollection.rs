use geoarrow_array::array::GeometryCollectionArray;
use geoarrow_array::builder::GeometryCollectionBuilder;
use geoarrow_array::error::Result;
use geoarrow_schema::GeometryCollectionType;

use crate::import::array::FromGEOS;
use crate::import::scalar::GEOSGeometryCollection;

impl FromGEOS for GeometryCollectionBuilder {
    type GeoArrowType = GeometryCollectionType;

    fn from_geos(
        geoms: impl IntoIterator<Item = Option<geos::Geometry>>,
        typ: Self::GeoArrowType,
    ) -> geoarrow_array::error::Result<Self> {
        let geoms = geoms
            .into_iter()
            .map(|geom| geom.map(GEOSGeometryCollection::try_new).transpose())
            .collect::<Result<Vec<_>>>()?;
        Self::from_nullable_geometry_collections(&geoms, typ)
    }
}

impl FromGEOS for GeometryCollectionArray {
    type GeoArrowType = GeometryCollectionType;

    fn from_geos(
        geoms: impl IntoIterator<Item = Option<geos::Geometry>>,
        typ: Self::GeoArrowType,
    ) -> Result<Self> {
        Ok(GeometryCollectionBuilder::from_geos(geoms, typ)?.finish())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::export::to_geos_geometry;

    use geoarrow_array::test::geometrycollection::array;
    use geoarrow_array::{GeoArrowArrayAccessor, IntoArrow};
    use geoarrow_schema::{CoordType, Dimension};

    #[ignore = "geometry collection import from GEOS not yet implemented"]
    #[test]
    fn geos_round_trip() {
        for coord_type in [CoordType::Interleaved, CoordType::Separated] {
            for dim in [Dimension::XY, Dimension::XYZ] {
                let arr = array(coord_type, dim, false);

                let geos_geoms = arr
                    .iter()
                    .map(|opt_x| opt_x.map(|x| to_geos_geometry(&x.unwrap()).unwrap()))
                    .collect::<Vec<_>>();
                let round_trip =
                    GeometryCollectionArray::from_geos(geos_geoms, arr.ext_type().clone()).unwrap();
                assert_eq!(arr, round_trip);
            }
        }
    }
}
