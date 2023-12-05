use arrow_array::OffsetSizeTrait;
use bumpalo::collections::CollectIn;

use crate::array::polygon::PolygonCapacity;
use crate::array::{PolygonArray, PolygonBuilder};
use crate::error::{GeoArrowError, Result};
use crate::io::geos::scalar::GEOSPolygon;
use geos::Geom;

// NOTE: this, `first_pass`, and `second_pass` are copied from their main implementations, because
// implementing geometry access traits on GEOS geometries that yield ConstGeometry objects with two
// lifetimes seemed really, really hard. Ideally one day we can unify the two branches!

impl<O: OffsetSizeTrait> PolygonBuilder<O> {
    /// Add a new GEOS Polygon to the end of this array.
    ///
    /// # Errors
    ///
    /// This function errors iff the new last item is larger than what O supports.
    fn push_geos_polygon(&mut self, value: Option<&GEOSPolygon>) -> Result<()> {
        if let Some(polygon) = value {
            if polygon.0.is_empty()? {
                self.push_empty();
                return Ok(());
            }

            // - Get exterior ring
            // - Add exterior ring's # of coords self.ring_offsets
            // - Push ring's coords to self.coords
            let ext_ring = polygon.0.get_exterior_ring()?;
            let ext_ring_num_coords = ext_ring.get_num_coordinates()?;
            self.ring_offsets.try_push_usize(ext_ring_num_coords)?;
            let coord_seq = ext_ring.get_coord_seq()?;
            for coord_idx in 0..ext_ring_num_coords {
                self.coords
                    .push_xy(coord_seq.get_x(coord_idx)?, coord_seq.get_y(coord_idx)?);
            }

            // Total number of rings in this polygon
            let num_interiors = polygon.0.get_num_interior_rings()?;
            self.geom_offsets.try_push_usize(num_interiors + 1)?;

            // For each interior ring:
            // - Get ring
            // - Add ring's # of coords to self.ring_offsets
            // - Push ring's coords to self.coords
            for int_ring_idx in 0..num_interiors {
                let int_ring = polygon
                    .0
                    .get_interior_ring_n(int_ring_idx.try_into().unwrap())?;
                let int_ring_num_coords = int_ring.get_num_coordinates()?;
                self.ring_offsets.try_push_usize(int_ring_num_coords)?;
                let coord_seq = int_ring.get_coord_seq()?;
                for coord_idx in 0..int_ring_num_coords {
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

fn first_pass(geoms: &[Option<GEOSPolygon>], geoms_length: usize) -> (usize, usize, usize) {
    // Total number of coordinates
    let mut coord_capacity = 0;
    let mut ring_capacity = 0;
    let geom_capacity = geoms_length;

    for polygon in geoms.iter().flatten() {
        // Total number of rings in this polygon
        let num_interiors = polygon.0.get_num_interior_rings().unwrap();
        ring_capacity += num_interiors + 1;

        // Number of coords for each ring
        if !polygon.0.is_empty().unwrap() {
            let exterior = polygon.0.get_exterior_ring().unwrap();
            coord_capacity += exterior.get_num_coordinates().unwrap();
        }

        for int_ring_idx in 0..polygon.0.get_num_interior_rings().unwrap() {
            let int_ring = polygon
                .0
                .get_interior_ring_n(int_ring_idx.try_into().unwrap())
                .unwrap();
            coord_capacity += int_ring.get_num_coordinates().unwrap();
        }
    }

    // TODO: dataclass for capacities to access them by name?
    (coord_capacity, ring_capacity, geom_capacity)
}

fn second_pass<'a, O: OffsetSizeTrait>(
    geoms: impl Iterator<Item = Option<GEOSPolygon<'a>>>,
    coord_capacity: usize,
    ring_capacity: usize,
    geom_capacity: usize,
) -> PolygonBuilder<O> {
    let capacity = PolygonCapacity::new(coord_capacity, ring_capacity, geom_capacity);
    let mut array = PolygonBuilder::with_capacity(capacity);

    geoms
        .into_iter()
        .try_for_each(|maybe_polygon| array.push_geos_polygon(maybe_polygon.as_ref()))
        .unwrap();

    array
}

impl<O: OffsetSizeTrait> TryFrom<Vec<Option<geos::Geometry<'_>>>> for PolygonBuilder<O> {
    type Error = GeoArrowError;

    fn try_from(value: Vec<Option<geos::Geometry<'_>>>) -> Result<Self> {
        let length = value.len();
        // TODO: don't use new_unchecked
        let geos_objects: Vec<Option<GEOSPolygon>> = value
            .into_iter()
            .map(|geom| geom.map(GEOSPolygon::new_unchecked))
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

impl<O: OffsetSizeTrait> TryFrom<Vec<Option<geos::Geometry<'_>>>> for PolygonArray<O> {
    type Error = GeoArrowError;

    fn try_from(value: Vec<Option<geos::Geometry<'_>>>) -> Result<Self> {
        let mutable_arr: PolygonBuilder<O> = value.try_into()?;
        Ok(mutable_arr.into())
    }
}

impl<'a, O: OffsetSizeTrait> TryFrom<bumpalo::collections::Vec<'a, Option<geos::Geometry<'_>>>>
    for PolygonBuilder<O>
{
    type Error = GeoArrowError;

    fn try_from(value: bumpalo::collections::Vec<'a, Option<geos::Geometry<'_>>>) -> Result<Self> {
        let bump = bumpalo::Bump::new();

        // TODO: avoid creating GEOSPolygon objects at all?
        let length = value.len();
        // TODO: don't use new_unchecked
        let geos_objects: bumpalo::collections::Vec<'_, Option<GEOSPolygon>> = value
            .into_iter()
            .map(|geom| geom.map(GEOSPolygon::new_unchecked))
            .collect_in(&bump);

        let (coord_capacity, ring_capacity, geom_capacity) = first_pass(&geos_objects, length);
        Ok(second_pass(
            geos_objects.into_iter(),
            coord_capacity,
            ring_capacity,
            geom_capacity,
        ))
    }
}

impl<'a, O: OffsetSizeTrait> TryFrom<bumpalo::collections::Vec<'a, Option<geos::Geometry<'_>>>>
    for PolygonArray<O>
{
    type Error = GeoArrowError;

    fn try_from(value: bumpalo::collections::Vec<'a, Option<geos::Geometry<'_>>>) -> Result<Self> {
        let mutable_arr: PolygonBuilder<O> = value.try_into()?;
        Ok(mutable_arr.into())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::test::polygon::p_array;

    #[test]
    fn geos_round_trip() {
        let arr = p_array();
        let geos_geoms: Vec<Option<geos::Geometry>> = arr.iter_geos().collect();
        let round_trip: PolygonArray<i32> = geos_geoms.try_into().unwrap();
        assert_eq!(arr, round_trip);
    }
}
