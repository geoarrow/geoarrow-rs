use arrow_array::OffsetSizeTrait;

use crate::array::linestring::LineStringCapacity;
use crate::array::{LineStringArray, LineStringBuilder};
use crate::error::{GeoArrowError, Result};
use crate::geo_traits::LineStringTrait;
use crate::io::geos::scalar::GEOSLineString;

// NOTE: this, `first_pass`, and `second_pass` are copied from their main implementations, because
// implementing geometry access traits on GEOS geometries that yield ConstGeometry objects with two
// lifetimes seemed really, really hard. Ideally one day we can unify the two branches!

impl<O: OffsetSizeTrait> LineStringBuilder<O> {
    /// Add a new GEOS LineString to the end of this array.
    ///
    /// # Errors
    ///
    /// This function errors iff the new last item is larger than what O supports.
    #[allow(dead_code)]
    fn push_geos_line_string(&mut self, value: Option<&GEOSLineString>) -> Result<()> {
        if let Some(line_string) = value {
            // For each ring:
            // - Get ring
            // - Add ring's # of coords to self.ring_offsets
            // - Push ring's coords to self.coords

            self.geom_offsets
                .try_push_usize(line_string.num_coords())
                .unwrap();

            for coord_idx in 0..line_string.num_coords() {
                let coord = line_string.coord(coord_idx).unwrap();
                self.coords.push_coord(&coord);
            }

            self.validity.append(true);
        } else {
            self.push_null();
        }
        Ok(())
    }
}

pub(crate) fn first_pass(geoms: &[Option<GEOSLineString>], geoms_length: usize) -> (usize, usize) {
    let mut coord_capacity = 0;
    let geom_capacity = geoms_length;

    for line_string in geoms.iter().flatten() {
        coord_capacity += line_string.num_coords();
    }

    (coord_capacity, geom_capacity)
}

pub(crate) fn second_pass<'a, O: OffsetSizeTrait>(
    geoms: impl Iterator<Item = Option<GEOSLineString<'a>>>,
    coord_capacity: usize,
    geom_capacity: usize,
) -> LineStringBuilder<O> {
    let capacity = LineStringCapacity::new(coord_capacity, geom_capacity);
    let mut array = LineStringBuilder::with_capacity(capacity);

    geoms
        .into_iter()
        .try_for_each(|maybe_multi_point| array.push_line_string(maybe_multi_point.as_ref()))
        .unwrap();

    array
}

impl<O: OffsetSizeTrait> TryFrom<Vec<Option<geos::Geometry<'_>>>> for LineStringBuilder<O> {
    type Error = GeoArrowError;

    fn try_from(value: Vec<Option<geos::Geometry<'_>>>) -> Result<Self> {
        let length = value.len();
        // TODO: don't use new_unchecked
        let geos_objects: Vec<Option<GEOSLineString>> = value
            .into_iter()
            .map(|geom| geom.map(GEOSLineString::new_unchecked))
            .collect();

        let (coord_capacity, geom_capacity) = first_pass(&geos_objects, length);
        Ok(second_pass(
            geos_objects.into_iter(),
            coord_capacity,
            geom_capacity,
        ))
    }
}

impl<'a, O: OffsetSizeTrait> TryFrom<Vec<Option<geos::Geometry<'a>>>> for LineStringArray<O> {
    type Error = GeoArrowError;

    fn try_from(value: Vec<Option<geos::Geometry<'a>>>) -> std::result::Result<Self, Self::Error> {
        let mutable_arr: LineStringBuilder<O> = value.try_into()?;
        Ok(mutable_arr.into())
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
