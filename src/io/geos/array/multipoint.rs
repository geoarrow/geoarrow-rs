use arrow_array::OffsetSizeTrait;

use crate::array::{MultiPointArray, MultiPointBuilder};
use crate::error::GeoArrowError;
use crate::io::geos::scalar::GEOSMultiPoint;

impl<O: OffsetSizeTrait, const D: usize> TryFrom<Vec<Option<geos::Geometry>>>
    for MultiPointBuilder<O, D>
{
    type Error = GeoArrowError;

    fn try_from(value: Vec<Option<geos::Geometry>>) -> std::result::Result<Self, Self::Error> {
        // TODO: don't use new_unchecked
        let geos_objects: Vec<Option<GEOSMultiPoint>> = value
            .into_iter()
            .map(|geom| geom.map(GEOSMultiPoint::new_unchecked))
            .collect();
        Ok(geos_objects.into())
    }
}

impl<O: OffsetSizeTrait, const D: usize> TryFrom<Vec<Option<geos::Geometry>>>
    for MultiPointArray<O, D>
{
    type Error = GeoArrowError;

    fn try_from(value: Vec<Option<geos::Geometry>>) -> std::result::Result<Self, Self::Error> {
        let mutable_arr: MultiPointBuilder<O, D> = value.try_into()?;
        Ok(mutable_arr.into())
    }
}

#[allow(unused_imports)]
#[cfg(test)]
mod test {
    use super::*;
    use crate::test::multipoint::mp_array;
    use crate::trait_::{GeometryArrayAccessor, GeometryScalarTrait};

    #[test]
    fn geos_round_trip() {
        let arr = mp_array();
        let geos_geoms: Vec<Option<geos::Geometry>> = arr
            .iter()
            .map(|opt_x| opt_x.map(|x| x.to_geos().unwrap()))
            .collect();
        let round_trip: MultiPointArray<i32, 2> = geos_geoms.try_into().unwrap();
        assert_eq!(arr, round_trip);
    }
}
