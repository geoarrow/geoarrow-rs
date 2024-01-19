use arrow_array::OffsetSizeTrait;

use crate::array::{LineStringArray, LineStringBuilder};
use crate::error::{GeoArrowError, Result};
use crate::io::geos::scalar::GEOSLineString;

impl<O: OffsetSizeTrait> TryFrom<Vec<Option<geos::Geometry<'_>>>> for LineStringBuilder<O> {
    type Error = GeoArrowError;

    fn try_from(value: Vec<Option<geos::Geometry<'_>>>) -> Result<Self> {
        // TODO: don't use new_unchecked
        let geos_objects: Vec<Option<GEOSLineString>> = value
            .into_iter()
            .map(|geom| geom.map(GEOSLineString::new_unchecked))
            .collect();
        Ok(geos_objects.into())
    }
}

impl<'a, O: OffsetSizeTrait> TryFrom<Vec<Option<geos::Geometry<'a>>>> for LineStringArray<O> {
    type Error = GeoArrowError;

    fn try_from(value: Vec<Option<geos::Geometry<'a>>>) -> std::result::Result<Self, Self::Error> {
        let mutable_arr: LineStringBuilder<O> = value.try_into()?;
        Ok(mutable_arr.into())
    }
}

#[allow(unused_imports)]
#[cfg(test)]
mod test {
    use super::*;
    use crate::test::linestring::ls_array;
    use crate::trait_::{GeometryArrayAccessor, GeometryScalarTrait};

    #[ignore = "geos lifetime error"]
    #[test]
    fn geos_round_trip() {
        let arr = ls_array();
        let _scalars = arr.iter().collect::<Vec<_>>();
        todo!()
        // let geos_geoms = scalars
        //     .iter()
        //     .map(|opt_x| opt_x.map(|x| x.to_geos().unwrap()))
        //     .collect::<Vec<_>>();
        // let round_trip: LineStringArray<i32> = geos_geoms.try_into().unwrap();
        // assert_eq!(arr, round_trip);
    }
}
