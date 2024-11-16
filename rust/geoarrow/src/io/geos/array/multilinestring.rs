use crate::array::{MultiLineStringArray, MultiLineStringBuilder};
use crate::datatypes::Dimension;
use crate::error::Result;
use crate::io::geos::scalar::GEOSMultiLineString;

impl MultiLineStringBuilder {
    pub fn from_geos(value: Vec<Option<geos::Geometry>>, dim: Dimension) -> Result<Self> {
        // TODO: don't use new_unchecked
        let geos_objects: Vec<Option<GEOSMultiLineString>> = value
            .into_iter()
            .map(|geom| geom.map(GEOSMultiLineString::new_unchecked))
            .collect();
        Ok((geos_objects, dim).into())
    }
}

impl MultiLineStringArray {
    pub fn from_geos(value: Vec<Option<geos::Geometry>>, dim: Dimension) -> Result<Self> {
        let mutable_arr = MultiLineStringBuilder::from_geos(value, dim)?;
        Ok(mutable_arr.into())
    }
}

#[allow(unused_imports)]
#[cfg(test)]
mod test {
    use super::*;
    use crate::test::multilinestring::ml_array;
    use crate::trait_::{ArrayAccessor, NativeScalar};

    #[test]
    fn geos_round_trip() {
        let arr = ml_array();
        let geos_geoms: Vec<Option<geos::Geometry>> = arr
            .iter()
            .map(|opt_x| opt_x.map(|x| x.to_geos().unwrap()))
            .collect();
        let round_trip = MultiLineStringArray::from_geos(geos_geoms, Dimension::XY).unwrap();
        assert_eq!(arr, round_trip);
    }
}
