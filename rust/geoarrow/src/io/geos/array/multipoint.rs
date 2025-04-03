use crate::array::{MultiPointArray, MultiPointBuilder};
use geoarrow_schema::Dimension;
use crate::error::Result;
use crate::io::geos::scalar::GEOSMultiPoint;

impl MultiPointBuilder {
    #[allow(dead_code)]
    pub(crate) fn from_geos(value: Vec<Option<geos::Geometry>>, dim: Dimension) -> Result<Self> {
        // TODO: don't use new_unchecked
        let geos_objects: Vec<Option<GEOSMultiPoint>> = value
            .into_iter()
            .map(|geom| geom.map(GEOSMultiPoint::new_unchecked))
            .collect();
        Ok((geos_objects, dim).into())
    }
}

impl MultiPointArray {
    #[allow(dead_code)]
    pub(crate) fn from_geos(value: Vec<Option<geos::Geometry>>, dim: Dimension) -> Result<Self> {
        let mutable_arr = MultiPointBuilder::from_geos(value, dim)?;
        Ok(mutable_arr.into())
    }
}

#[allow(unused_imports)]
#[cfg(test)]
mod test {
    use super::*;
    use crate::test::multipoint::mp_array;
    use crate::trait_::{ArrayAccessor, NativeScalar};

    #[test]
    fn geos_round_trip() {
        let arr = mp_array();
        let geos_geoms: Vec<Option<geos::Geometry>> = arr
            .iter()
            .map(|opt_x| opt_x.map(|x| x.to_geos().unwrap()))
            .collect();
        let round_trip = MultiPointArray::from_geos(geos_geoms, Dimension::XY).unwrap();
        assert_eq!(arr, round_trip);
    }
}
