use arrow_array::OffsetSizeTrait;

use crate::array::{MultiPolygonArray, MultiPolygonBuilder};
use crate::error::{GeoArrowError, Result};
use crate::io::geos::scalar::GEOSMultiPolygon;

impl<O: OffsetSizeTrait, const D: usize> TryFrom<Vec<Option<geos::Geometry>>>
    for MultiPolygonBuilder<O, D>
{
    type Error = GeoArrowError;

    fn try_from(value: Vec<Option<geos::Geometry>>) -> Result<Self> {
        // TODO: don't use new_unchecked
        let geos_objects: Vec<Option<GEOSMultiPolygon>> = value
            .into_iter()
            .map(|geom| geom.map(GEOSMultiPolygon::new_unchecked))
            .collect();
        Ok(geos_objects.into())
    }
}

impl<O: OffsetSizeTrait, const D: usize> TryFrom<Vec<Option<geos::Geometry>>>
    for MultiPolygonArray<O, D>
{
    type Error = GeoArrowError;

    fn try_from(value: Vec<Option<geos::Geometry>>) -> Result<Self> {
        let mutable_arr: MultiPolygonBuilder<O, D> = value.try_into()?;
        Ok(mutable_arr.into())
    }
}

#[allow(unused_imports)]
#[cfg(test)]
mod test {
    use super::*;
    use crate::test::multipolygon::mp_array;
    use crate::trait_::{NativeArrayAccessor, NativeScalar};

    #[test]
    fn geos_round_trip() {
        let arr = mp_array();
        let geos_geoms: Vec<Option<geos::Geometry>> = arr
            .iter()
            .map(|opt_x| opt_x.map(|x| x.to_geos().unwrap()))
            .collect();
        let round_trip: MultiPolygonArray<i32, 2> = geos_geoms.try_into().unwrap();
        assert_eq!(arr, round_trip);
    }
}
