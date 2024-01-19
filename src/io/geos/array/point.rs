use crate::array::{PointArray, PointBuilder};
use crate::error::GeoArrowError;
use crate::io::geos::scalar::GEOSPoint;

impl<'a> TryFrom<Vec<Option<geos::Geometry<'a>>>> for PointBuilder {
    type Error = GeoArrowError;

    fn try_from(value: Vec<Option<geos::Geometry<'a>>>) -> std::result::Result<Self, Self::Error> {
        // TODO: don't use new_unchecked
        let geos_linestring_objects: Vec<Option<GEOSPoint>> = value
            .into_iter()
            .map(|geom| geom.map(GEOSPoint::new_unchecked))
            .collect();
        Ok(geos_linestring_objects.into())
    }
}

impl<'a> TryFrom<Vec<Option<geos::Geometry<'a>>>> for PointArray {
    type Error = GeoArrowError;

    fn try_from(value: Vec<Option<geos::Geometry<'a>>>) -> std::result::Result<Self, Self::Error> {
        let mutable_arr: PointBuilder = value.try_into()?;
        Ok(mutable_arr.into())
    }
}

#[allow(unused_imports)]
#[cfg(test)]
mod test {
    use super::*;
    use crate::test::point::point_array;
    use crate::trait_::{GeometryArrayAccessor, GeometryScalarTrait};

    #[ignore = "geos lifetime error"]
    #[test]
    fn geos_round_trip() {
        let _arr = point_array();
        todo!()

        // let geos_geoms: Vec<Option<geos::Geometry>> = arr
        //     .iter()
        //     .map(|opt_x| opt_x.map(|x| x.to_geos().unwrap()))
        //     .collect();
        // let round_trip: PointArray = geos_geoms.try_into().unwrap();
        // assert_eq!(arr, round_trip);
    }
}
