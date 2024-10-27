use crate::array::{MultiLineStringArray, MultiLineStringBuilder};
use crate::error::Result;
use crate::io::geos::scalar::GEOSMultiLineString;

impl<const D: usize> MultiLineStringBuilder<D> {
    fn from_geos(value: Vec<Option<geos::Geometry>>) -> Result<Self> {
        // TODO: don't use new_unchecked
        let geos_objects: Vec<Option<GEOSMultiLineString>> = value
            .into_iter()
            .map(|geom| geom.map(GEOSMultiLineString::new_unchecked))
            .collect();
        Ok(geos_objects.into())
    }
}

impl<const D: usize> MultiLineStringArray<D> {
    fn from_geos(value: Vec<Option<geos::Geometry>>) -> Result<Self> {
        let mutable_arr = MultiLineStringBuilder::from_geos(value)?;
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
        let round_trip = MultiLineStringArray::<2>::from_geos(geos_geoms).unwrap();
        assert_eq!(arr, round_trip);
    }
}
