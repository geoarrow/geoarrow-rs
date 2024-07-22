use arrow_array::OffsetSizeTrait;

use crate::array::{PolygonArray, PolygonBuilder};
use crate::error::{GeoArrowError, Result};
use crate::io::geos::scalar::GEOSPolygon;

impl<O: OffsetSizeTrait> TryFrom<Vec<Option<geos::Geometry>>> for PolygonBuilder<O, 2> {
    type Error = GeoArrowError;

    fn try_from(value: Vec<Option<geos::Geometry>>) -> Result<Self> {
        // TODO: don't use new_unchecked
        let geos_objects: Vec<Option<GEOSPolygon>> = value
            .into_iter()
            .map(|geom| geom.map(GEOSPolygon::new_unchecked))
            .collect();

        Ok(geos_objects.into())
    }
}

impl<O: OffsetSizeTrait> TryFrom<Vec<Option<geos::Geometry>>> for PolygonArray<O, 2> {
    type Error = GeoArrowError;

    fn try_from(value: Vec<Option<geos::Geometry>>) -> Result<Self> {
        let mutable_arr: PolygonBuilder<O, 2> = value.try_into()?;
        Ok(mutable_arr.into())
    }
}

#[allow(unused_imports)]
#[cfg(test)]
mod test {
    use super::*;
    use crate::test::polygon::p_array;
    use crate::trait_::{GeometryArrayAccessor, GeometryScalarTrait};

    #[ignore = "geos lifetime error"]
    #[test]
    fn geos_round_trip() {
        let _arr = p_array();
        todo!()

        // let geos_geoms: Vec<Option<geos::Geometry>> = arr
        //     .iter()
        //     .map(|opt_x| opt_x.map(|x| x.to_geos().unwrap()))
        //     .collect();
        // let round_trip: PolygonArray<i32> = geos_geoms.try_into().unwrap();
        // assert_eq!(arr, round_trip);
    }
}
