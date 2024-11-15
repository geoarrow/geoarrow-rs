use crate::array::{PointArray, PointBuilder};
use crate::error::Result;
use crate::io::geos::scalar::GEOSPoint;

impl PointBuilder<D> {
    pub fn from_geos(value: Vec<Option<geos::Geometry>>) -> Result<Self> {
        // TODO: don't use new_unchecked
        let geos_linestring_objects: Vec<Option<GEOSPoint>> = value
            .into_iter()
            .map(|geom| geom.map(GEOSPoint::new_unchecked))
            .collect();
        Ok(geos_linestring_objects.into())
    }
}

impl PointArray<D> {
    pub fn from_geos(value: Vec<Option<geos::Geometry>>) -> Result<Self> {
        let mutable_arr = PointBuilder::from_geos(value)?;
        Ok(mutable_arr.into())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::test::point::point_array;
    use crate::trait_::{ArrayAccessor, NativeScalar};

    #[test]
    fn geos_round_trip() {
        let arr = point_array();
        let geos_geoms: Vec<Option<geos::Geometry>> = arr
            .iter()
            .map(|opt_x| opt_x.map(|x| x.to_geos().unwrap()))
            .collect();
        let round_trip = PointArray::<2>::from_geos(geos_geoms).unwrap();
        assert_eq!(arr, round_trip);
    }
}
