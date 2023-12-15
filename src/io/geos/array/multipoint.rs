use arrow_array::OffsetSizeTrait;

use crate::array::{MultiPointArray, MultiPointBuilder};
use crate::error::GeoArrowError;
use crate::io::geos::scalar::GEOSMultiPoint;

impl<'a, O: OffsetSizeTrait> TryFrom<Vec<Option<geos::Geometry<'a>>>> for MultiPointBuilder<O> {
    type Error = GeoArrowError;

    fn try_from(value: Vec<Option<geos::Geometry<'a>>>) -> std::result::Result<Self, Self::Error> {
        // TODO: don't use new_unchecked
        let geos_objects: Vec<Option<GEOSMultiPoint>> = value
            .into_iter()
            .map(|geom| geom.map(GEOSMultiPoint::new_unchecked))
            .collect();
        Ok(geos_objects.into())
    }
}

impl<'a, O: OffsetSizeTrait> TryFrom<Vec<Option<geos::Geometry<'a>>>> for MultiPointArray<O> {
    type Error = GeoArrowError;

    fn try_from(value: Vec<Option<geos::Geometry<'a>>>) -> std::result::Result<Self, Self::Error> {
        let mutable_arr: MultiPointBuilder<O> = value.try_into()?;
        Ok(mutable_arr.into())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::test::multipoint::mp_array;

    #[test]
    fn geos_round_trip() {
        let arr = mp_array();
        let geos_geoms: Vec<Option<geos::Geometry>> = arr.iter_geos().collect();
        let round_trip: MultiPointArray<i32> = geos_geoms.try_into().unwrap();
        assert_eq!(arr, round_trip);
    }
}
