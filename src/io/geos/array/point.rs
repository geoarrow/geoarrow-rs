use crate::array::point::mutable::from_nullable_coords;
use crate::array::{MutablePointArray, PointArray};
use crate::error::GeoArrowError;
use crate::io::geos::scalar::GEOSPoint;

impl<'a> TryFrom<Vec<Option<geos::Geometry<'a>>>> for MutablePointArray {
    type Error = GeoArrowError;

    fn try_from(value: Vec<Option<geos::Geometry<'a>>>) -> std::result::Result<Self, Self::Error> {
        let length = value.len();
        // TODO: don't use new_unchecked
        let geos_linestring_objects: Vec<Option<GEOSPoint>> = value
            .into_iter()
            .map(|geom| geom.map(GEOSPoint::new_unchecked))
            .collect();
        Ok(from_nullable_coords(
            geos_linestring_objects.iter().map(|item| item.as_ref()),
            length,
        ))
    }
}

impl<'a> TryFrom<Vec<Option<geos::Geometry<'a>>>> for PointArray {
    type Error = GeoArrowError;

    fn try_from(value: Vec<Option<geos::Geometry<'a>>>) -> std::result::Result<Self, Self::Error> {
        let mutable_arr: MutablePointArray = value.try_into()?;
        Ok(mutable_arr.into())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::test::point::point_array;

    #[test]
    fn geos_round_trip() {
        let arr = point_array();
        let geos_geoms: Vec<Option<geos::Geometry>> = arr.iter_geos().collect();
        let round_trip: PointArray = geos_geoms.try_into().unwrap();
        assert_eq!(arr, round_trip);
    }
}
