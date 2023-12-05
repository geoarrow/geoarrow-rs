use arrow_array::OffsetSizeTrait;

use crate::array::multilinestring::MultiLineStringCapacity;
use crate::array::{MultiLineStringArray, MultiLineStringBuilder};
use crate::error::{GeoArrowError, Result};
use crate::geo_traits::LineStringTrait;
use crate::io::geos::scalar::{GEOSLineString, GEOSMultiLineString};
use geos::Geom;

// NOTE: this, `first_pass`, and `second_pass` are copied from their main implementations, because
// implementing geometry access traits on GEOS geometries that yield ConstGeometry objects with two
// lifetimes seemed really, really hard. Ideally one day we can unify the two branches!

impl<O: OffsetSizeTrait> MultiLineStringBuilder<O> {
    /// Add a new GEOS LineString to the end of this array.
    ///
    /// # Errors
    ///
    /// This function errors iff the new last item is larger than what O supports.
    #[allow(dead_code)]
    fn push_geos_line_string(&mut self, value: Option<&GEOSLineString>) -> Result<()> {
        if let Some(line_string) = value {
            // Total number of linestrings in this multilinestring
            let num_line_strings = 1;
            self.geom_offsets.try_push_usize(num_line_strings)?;

            // For each ring:
            // - Get ring
            // - Add ring's # of coords to self.ring_offsets
            // - Push ring's coords to self.coords

            self.ring_offsets
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

    /// Add a new GEOS MultiLineString to the end of this array.
    ///
    /// # Errors
    ///
    /// This function errors iff the new last item is larger than what O supports.
    fn push_geos_multi_line_string(&mut self, value: Option<&GEOSMultiLineString>) -> Result<()> {
        if let Some(multi_line_string) = value {
            // Total number of linestrings in this multilinestring
            let num_line_strings = multi_line_string.num_lines();
            self.geom_offsets.try_push_usize(num_line_strings)?;

            // For each ring:
            // - Get ring
            // - Add ring's # of coords to self.ring_offsets
            // - Push ring's coords to self.coords

            // Number of coords for each ring
            for line_string_idx in 0..num_line_strings {
                let line_string = multi_line_string.0.get_geometry_n(line_string_idx).unwrap();
                let line_string_num_coords = line_string.get_num_coordinates()?;
                self.ring_offsets
                    .try_push_usize(line_string_num_coords)
                    .unwrap();
                let coord_seq = line_string.get_coord_seq()?;
                for coord_idx in 0..line_string_num_coords {
                    self.coords
                        .push_xy(coord_seq.get_x(coord_idx)?, coord_seq.get_y(coord_idx)?);
                }
            }

            self.validity.append(true);
        } else {
            self.push_null();
        }
        Ok(())
    }
}

fn first_pass(geoms: &[Option<GEOSMultiLineString>], geoms_length: usize) -> (usize, usize, usize) {
    // Total number of coordinates
    let mut coord_capacity = 0;
    let mut ring_capacity = 0;
    let geom_capacity = geoms_length;

    for multi_line_string in geoms.iter().flatten() {
        // Total number of rings in this polygon
        let num_line_strings = multi_line_string.num_lines();
        ring_capacity += num_line_strings;

        for line_string_idx in 0..num_line_strings {
            let line_string = multi_line_string.0.get_geometry_n(line_string_idx).unwrap();
            coord_capacity += line_string.get_num_coordinates().unwrap();
        }
    }

    // TODO: dataclass for capacities to access them by name?
    (coord_capacity, ring_capacity, geom_capacity)
}

fn second_pass<'a, O: OffsetSizeTrait>(
    geoms: impl Iterator<Item = Option<GEOSMultiLineString<'a>>>,
    coord_capacity: usize,
    ring_capacity: usize,
    geom_capacity: usize,
) -> MultiLineStringBuilder<O> {
    let capacity = MultiLineStringCapacity::new(coord_capacity, ring_capacity, geom_capacity);
    let mut array = MultiLineStringBuilder::with_capacity(capacity);

    geoms
        .into_iter()
        .try_for_each(|maybe_multi_line_string| {
            array.push_geos_multi_line_string(maybe_multi_line_string.as_ref())
        })
        .unwrap();

    array
}

impl<O: OffsetSizeTrait> TryFrom<Vec<Option<geos::Geometry<'_>>>> for MultiLineStringBuilder<O> {
    type Error = GeoArrowError;

    fn try_from(value: Vec<Option<geos::Geometry<'_>>>) -> Result<Self> {
        let length = value.len();
        // TODO: don't use new_unchecked
        let geos_objects: Vec<Option<GEOSMultiLineString>> = value
            .into_iter()
            .map(|geom| geom.map(GEOSMultiLineString::new_unchecked))
            .collect();

        let (coord_capacity, ring_capacity, geom_capacity) = first_pass(&geos_objects, length);
        Ok(second_pass(
            geos_objects.into_iter(),
            coord_capacity,
            ring_capacity,
            geom_capacity,
        ))
    }
}

impl<O: OffsetSizeTrait> TryFrom<Vec<Option<geos::Geometry<'_>>>> for MultiLineStringArray<O> {
    type Error = GeoArrowError;

    fn try_from(value: Vec<Option<geos::Geometry<'_>>>) -> Result<Self> {
        let mutable_arr: MultiLineStringBuilder<O> = value.try_into()?;
        Ok(mutable_arr.into())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::test::multilinestring::ml_array;

    #[test]
    fn geos_round_trip() {
        let arr = ml_array();
        let geos_geoms: Vec<Option<geos::Geometry>> = arr.iter_geos().collect();
        let round_trip: MultiLineStringArray<i32> = geos_geoms.try_into().unwrap();
        assert_eq!(arr, round_trip);
    }
}
