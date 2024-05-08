use crate::array::{PointArray, PointBuilder};
use crate::error::GeoArrowError;
use crate::io::geos::scalar::GEOSPoint;

impl TryFrom<Vec<Option<geos::Geometry>>> for PointBuilder {
    type Error = GeoArrowError;

    fn try_from(value: Vec<Option<geos::Geometry>>) -> std::result::Result<Self, Self::Error> {
        // TODO: don't use new_unchecked
        let geos_linestring_objects: Vec<Option<GEOSPoint>> = value
            .into_iter()
            .map(|geom| geom.map(GEOSPoint::new_unchecked))
            .collect();
        Ok(geos_linestring_objects.into())
    }
}

impl TryFrom<Vec<Option<geos::Geometry>>> for PointArray {
    type Error = GeoArrowError;

    fn try_from(value: Vec<Option<geos::Geometry>>) -> std::result::Result<Self, Self::Error> {
        let mutable_arr: PointBuilder = value.try_into()?;
        Ok(mutable_arr.into())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::test::point::point_array;
    use crate::trait_::{GeometryArrayAccessor, GeometryScalarTrait};

    #[test]
    fn geos_round_trip() {
        let arr = point_array();
        let geos_geoms: Vec<Option<geos::Geometry>> = arr
            .iter()
            .map(|opt_x| opt_x.map(|x| x.to_geos().unwrap()))
            .collect();
        let round_trip: PointArray = geos_geoms.try_into().unwrap();
        assert_eq!(arr, round_trip);
    }
}
