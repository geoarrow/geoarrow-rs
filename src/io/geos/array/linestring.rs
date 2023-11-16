use arrow_array::OffsetSizeTrait;

// use crate::array::linestring::mutable::{first_pass, second_pass};
// use crate::array::MutableLineStringArray;
use crate::array::LineStringArray;
use crate::error::GeoArrowError;
// use crate::io::geos::scalar::{GEOSLineString, GEOSConstLineString};

// impl<'a, O: OffsetSizeTrait> TryFrom<Vec<Option<geos::Geometry<'a>>>>
//     for MutableLineStringArray<O>
// {
//     type Error = GeoArrowError;

//     fn try_from(value: Vec<Option<geos::Geometry<'a>>>) -> std::result::Result<Self, Self::Error> {
//         // todo!()
//         let length = value.len();
//         // TODO: don't use new_unchecked
//         let geos_linestring_objects: Vec<Option<GEOSLineString>> = value
//             .into_iter()
//             .map(|geom| geom.map(GEOSLineString::new_unchecked))
//             .collect();
//         let (coord_capacity, geom_capacity) = first_pass(
//             geos_linestring_objects.iter().map(|item| item.as_ref()),
//             length,
//         );
//         Ok(second_pass(
//             geos_linestring_objects.into_iter(),
//             coord_capacity,
//             geom_capacity,
//         ))
//     }
// }

// impl<'a, 'b, O: OffsetSizeTrait> TryFrom<Vec<Option<geos::ConstGeometry<'a, 'b>>>>
//     for MutableLineStringArray<O>
// {
//     type Error = GeoArrowError;

//     fn try_from(
//         value: Vec<Option<geos::ConstGeometry<'a, 'b>>>,
//     ) -> std::result::Result<Self, Self::Error> {
//         // todo!()
//         let length = value.len();
//         // TODO: don't use new_unchecked
//         let geos_linestring_objects: Vec<Option<GEOSConstLineString>> = value
//             .into_iter()
//             .map(|geom| geom.as_ref().map(GEOSConstLineString::new_unchecked))
//             .collect();
//         let (coord_capacity, geom_capacity) = first_pass(
//             geos_linestring_objects.iter().map(|item| item.as_ref()),
//             length,
//         );
//         Ok(second_pass(
//             geos_linestring_objects.into_iter(),
//             coord_capacity,
//             geom_capacity,
//         ))
//     }
// }

impl<'a, O: OffsetSizeTrait> TryFrom<Vec<Option<geos::Geometry<'a>>>> for LineStringArray<O> {
    type Error = GeoArrowError;

    fn try_from(_value: Vec<Option<geos::Geometry<'a>>>) -> std::result::Result<Self, Self::Error> {
        todo!()
        // let mutable_arr: MutableLineStringArray<O> = value.try_into()?;
        // Ok(mutable_arr.into())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::test::linestring::ls_array;

    #[test]
    fn geos_round_trip() {
        let arr = ls_array();
        let geos_geoms: Vec<Option<geos::Geometry>> = arr.iter_geos().collect();
        let round_trip: LineStringArray<i32> = geos_geoms.try_into().unwrap();
        assert_eq!(arr, round_trip);
    }
}
