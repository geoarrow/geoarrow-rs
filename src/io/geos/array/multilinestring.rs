use arrow_array::OffsetSizeTrait;

use crate::array::{MultiLineStringArray, MultiLineStringBuilder};
use crate::error::{GeoArrowError, Result};
use crate::io::geos::scalar::GEOSMultiLineString;

impl<O: OffsetSizeTrait> TryFrom<Vec<Option<geos::Geometry<'_>>>> for MultiLineStringBuilder<O> {
    type Error = GeoArrowError;

    fn try_from(value: Vec<Option<geos::Geometry<'_>>>) -> Result<Self> {
        // TODO: don't use new_unchecked
        let geos_objects: Vec<Option<GEOSMultiLineString>> = value
            .into_iter()
            .map(|geom| geom.map(GEOSMultiLineString::new_unchecked))
            .collect();
        Ok(geos_objects.into())
    }
}

impl<O: OffsetSizeTrait> TryFrom<Vec<Option<geos::Geometry<'_>>>> for MultiLineStringArray<O> {
    type Error = GeoArrowError;

    fn try_from(value: Vec<Option<geos::Geometry<'_>>>) -> Result<Self> {
        let mutable_arr: MultiLineStringBuilder<O> = value.try_into()?;
        Ok(mutable_arr.into())
    }
}

#[allow(unused_imports)]
#[cfg(test)]
mod test {
    use super::*;
    use crate::test::multilinestring::ml_array;
    use crate::trait_::{GeometryArrayAccessor, GeometryScalarTrait};

    #[ignore = "geos lifetime error"]
    #[test]
    fn geos_round_trip() {
        let _arr = ml_array();
        todo!()

        // let geos_geoms: Vec<Option<geos::Geometry>> = arr
        //     .iter()
        //     .map(|opt_x| opt_x.map(|x| x.to_geos().unwrap()))
        //     .collect();
        // let round_trip: MultiLineStringArray<i32> = geos_geoms.try_into().unwrap();
        // assert_eq!(arr, round_trip);
    }
}
